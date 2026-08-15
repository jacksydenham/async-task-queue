/**
 * In-memory async task queue with a concurrency limit,
 * exponential backoff retries, and a dead-letter queue.
 */

export type TaskStatus = 'pending' | 'processing' | 'completed' | 'failed';

// Per-queue or per-task retry / backoff config
export interface RetryOptions {
  maxRetries: number;
  // Base delay in ms before the first retry (e.g. 1000).
  baseDelayMs: number;
  // Cap on the exponential delay in ms (e.g. 10000).
  maxDelayMs: number;
  // Full Jitter applied to the delay when true.
  useJitter: boolean;
}


// Error type which covers non-retryable failures via `isFatal`. 
// If isFatal, Skip remaining retries and go straight to DLQ
export class TaskError extends Error {
  isFatal?: boolean;

  constructor(message: string, isFatal?: boolean) {
    super(message);
    this.name = 'TaskError';
    if (isFatal !== undefined) {
      this.isFatal = isFatal;
    }
  }
}

// Generic task interface, execute is the async function we'll process.
export interface Task<T> {
  id: string;
  execute: () => Promise<T>;
  status: TaskStatus;
  // Current attempt count. Starts at 0 and increments on each retry.
  retryCount: number;
  // Last failure error, if any.
  error?: Error;
  // Per-task override of queue-level retry options.
  retryOptions?: Partial<RetryOptions>;
}

export interface QueueOptions {
  concurrency: number;
  // Optional default retry options applied to every task.
  retryOptions?: RetryOptions;
}

// Defaults used when neither the queue nor task supply a value.
const DEFAULT_RETRY_OPTIONS: RetryOptions = {
  maxRetries: 0,
  baseDelayMs: 1000,
  maxDelayMs: 10000,
  useJitter: true,
};

// pass/fail functions attached to our tasks once queued for promise fulfillment.
interface QueuedTask<T> extends Task<T> {
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
}

// Callback type for subs of failed tasks
type TaskFailedListener = (task: Task<unknown>, error: Error) => void;

export class AsyncTaskQueue {
  private readonly concurrency: number;
  private readonly defaultRetryOptions?: RetryOptions;
  private readonly taskFailedListeners = new Set<TaskFailedListener>();

  // Our queue of generic task objects
  private readonly pending: QueuedTask<unknown>[] = [];

  /**
   * Dead-letter queue: tasks that exhausted retries or failed fatally.
   * These are offloaded here so they no longer consume concurrency slots.
   * Currently the DLQ has no operation beyond returning a copy.
   */
  private readonly deadLetterQueue: Task<unknown>[] = [];

  // Simple concurrency hard-limit. How many runners can be active.
  private activeCount = 0;

  // Basic validation of queueOptions
  constructor(options: QueueOptions) {
    if (options.concurrency < 1) {
      throw new Error('concurrency must be at least 1');
    }

    this.concurrency = options.concurrency;
    
    // Set queue defaults for retryOptions
    if (options.retryOptions !== undefined) {
      this.defaultRetryOptions = options.retryOptions;
    }
  }

  /**
   * Subscribe to queue events. Only supports `'task:failed'` right now, 
   * emitted when a task is permanently failed and moved to the DLQ.
   */
  on(event: 'task:failed', listener: TaskFailedListener): this {
    if (event === 'task:failed') {
      this.taskFailedListeners.add(listener);
    }
    return this;
  }

  // Unsubscribe a registered listener.
  off(event: 'task:failed', listener: TaskFailedListener): this {
    if (event === 'task:failed') {
      this.taskFailedListeners.delete(listener);
    }
    return this;
  }

  // Returns a shallow copy of the dead-letter queue so callers cannot mutate the true DLQ array.
  getDeadLetterQueue(): Task<unknown>[] {
    return [...this.deadLetterQueue];
  }

  // Entry point for queueing tasks. Producers pass their function into push
  // and receive a promise that resolves/rejects when the task finishes
  // (including after any retries).
  push<T>(
    id: string,
    execute: () => Promise<T>,
    retryOptions?: Partial<RetryOptions>,
  ): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const task: QueuedTask<T> = {
        id,
        execute,
        status: 'pending',
        retryCount: 0,
        resolve,
        reject,
      };

      if (retryOptions !== undefined) {
        task.retryOptions = retryOptions;
      }

      this.pending.push(task as QueuedTask<unknown>);
      this.processQueue();
    });
  }

  /**
   * Exponential backoff with optional Full Jitter.
   *
   * exponentialDelay = min(maxDelayMs, baseDelayMs * 2^attempt)
   * - useJitter true  → random float in [0, exponentialDelay)  (Full Jitter)
   * - useJitter false → exponentialDelay as-is
   */
  private calculateBackoffDelay(attempt: number, options: RetryOptions): number {
    const exponentialDelay = Math.min(
      options.maxDelayMs,
      options.baseDelayMs * 2 ** attempt,
    );

    if (options.useJitter) {
      return Math.random() * exponentialDelay;
    }

    return exponentialDelay;
  }

  // Merge built-in defaults, queue defaults, and per-task overrides.
  private resolveRetryOptions(task: QueuedTask<unknown>): RetryOptions {
    return {
      ...DEFAULT_RETRY_OPTIONS,
      ...this.defaultRetryOptions,
      ...task.retryOptions,
    };
  }

  private toError(error: unknown): Error {
    if (error instanceof Error) {
      return error;
    }

    return new Error(String(error));
  }

  private isFatalError(error: unknown): boolean {
    return (
      typeof error === 'object' &&
      error !== null &&
      'isFatal' in error &&
      (error as { isFatal?: boolean }).isFatal === true
    );
  }

  private emitTaskFailed(task: Task<unknown>, error: Error): void {
    for (const listener of this.taskFailedListeners) {
      listener(task, error);
    }
  }

  private processQueue(): void {
    // Concurrency check to start workers only while activeCount is below the limit
    // and there are pending tasks waiting in our queue.
    while (this.activeCount < this.concurrency && this.pending.length > 0) {
      const task = this.pending.shift();
      if (task === undefined) {
        break;
      }

      // Void as we init a runner; we don't await the promise this creates.
      void this.runTask(task);
    }
  }

  // Runners operate async as we await them
  private async runTask(task: QueuedTask<unknown>): Promise<void> {
    // Increment our concurrency tracker.
    this.activeCount++;
    task.status = 'processing';

    try {
      const result = await task.execute();
      task.status = 'completed';
      task.resolve(result);
    } catch (error: unknown) {
      const normalizedError = this.toError(error);
      task.error = normalizedError;

      const retryOptions = this.resolveRetryOptions(task);
      const isFatal = this.isFatalError(error);

      // DLQ: fatal errors / exhausted retries leave the live queue as to not block concurrency slots
      if (isFatal || task.retryCount >= retryOptions.maxRetries) {
        task.status = 'failed';
        this.deadLetterQueue.push(task);
        this.emitTaskFailed(task, normalizedError);
        task.reject(normalizedError);
      } else {
        // Schedule a retry after exponential (optionally jittered) backoff.
        // Concurrency slot is freed in `finally` so other tasks keep moving while this one waits out its delay.
        task.retryCount++;
        const delayMs = this.calculateBackoffDelay(
          task.retryCount - 1,
          retryOptions,
        );
        task.status = 'pending';

        setTimeout(() => {
          this.pending.push(task);
          this.processQueue();
        }, delayMs);
      }
    } finally {
      // Concurrency update, worker slot is freed.
      this.activeCount--;

      // Immediately start next task if a slot is open.
      this.processQueue();
    }
  }
}
