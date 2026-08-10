/**
 * In-memory async task queue with a concurrency limit.
 * Just a skeleton, so no persistence, retries, cancellation, or max queue size yet.
 */

export type TaskStatus = 'pending' | 'processing' | 'completed' | 'failed';

// Generic task interface, execute is the async function we'll process.
export interface Task<T> {
  id: string;
  execute: () => Promise<T>;
  status: TaskStatus;
}

// Solely our concurrency limit.
export interface QueueOptions {
  concurrency: number;
}

// pass/fail functions attached to our tasks once queued for promise fulfillment.
interface QueuedTask<T> extends Task<T> {
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
}

export class AsyncTaskQueue {
  private readonly concurrency: number;

  // Our queue of generic task objects
  private readonly pending: QueuedTask<unknown>[] = [];

  // Simple concurrency hard-limit. How many runners can be active.
  private activeCount = 0;

  // Basic validation of queueOptions
  constructor(options: QueueOptions) {
    if (options.concurrency < 1) {
      throw new Error('concurrency must be at least 1');
    }
    this.concurrency = options.concurrency;
  }

  // Our entry point for queueing tasks. Producers will pass their function into push and receive 
  // a promise immediately which will resolve with a value or error.
  push<T>(id: string, execute: () => Promise<T>): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const task: QueuedTask<T> = {
        id,
        execute,
        status: 'pending',
        resolve,
        reject,
      };

      this.pending.push(task as QueuedTask<unknown>);
      this.processQueue();
    });
  }

  private processQueue(): void {
    // Concurrency check to start workers only while activeCount is below the limit
    // and there are pending tasks waiting in our queue.
    while (this.activeCount < this.concurrency && this.pending.length > 0) {
      const task = this.pending.shift();
      if (task === undefined) {
        break;
      }

      // Void as we init a runner, as we don't care for the promise this will create.
      void this.runTask(task);
    }
  }

  // Runners operate async as we await them
  private async runTask(task: QueuedTask<unknown>): Promise<void> {
    // Increment our concurrency tracker.
    this.activeCount++;
    task.status = 'processing';

    // If given fn succeeds, the caller's promise obj settles with its value.
    // Else reject it with error for caller to catch.
    try {
      const result = await task.execute();
      task.status = 'completed';
      task.resolve(result);
    } catch (error: unknown) {
      task.status = 'failed';
      task.reject(error);
    } finally {
      // Concurrency update, worker slot is freed.
      this.activeCount--;

      // Immediately start next task if a slot is open.
      this.processQueue();
    }
  }
}
