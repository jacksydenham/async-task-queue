# TypeScript Concurrent Async Task Queue Engine

A lightweight, zero-dependency, type-safe in-memory task queue engine built in TypeScript. Designed to explore asynchronous execution limits, V8 heap memory usage, concurrency control, and backend state transitions inside a single Node.js process.

---

## 🎯 Architectural Intent

This project explores backend concurrency mechanics and microservice communication patterns running inside a single Node.js process:

1. **Concurrency Regulation:** Enforces strict limits on active asynchronous workers to prevent connection pool and API rate exhaustion.
2. **Resilience & Fault Tolerance:** Manages transient errors using exponential backoff, jitter, and dead-letter queues (DLQ).
3. **Backpressure Control:** Implements modular ingestion strategies (Watermarks, Token Buckets, and Fail-Fast policies) to protect V8 heap memory.

---

## 🌐 Real-World Production Mapping

The mechanisms in this project map directly to enterprise backend infrastructure components:

| Project Component | Industry Equivalent | Concrete Real-World Use Case |
| :--- | :--- | :--- |
| **Worker Queue & Slots** | **AWS SQS / BullMQ** | Offloading heavy PDF generation or email dispatch off the main HTTP request thread. |
| **Concurrency Controls** | **PgBouncer / DB Connection Pools** | Protecting databases from hitting `Too many connections` under sudden traffic spikes. |
| **Memory Heap Retention** | **Node.js Memory Management** | Tracking active task references on the V8 Heap to prevent process crashes (`ERR_OUT_OF_MEMORY`). |

---

## 🛠 Project Status & Roadmap

- [x] **Stage 1: Core Concurrent Engine**
  - Strict TypeScript generic interfaces (`Task<T>`, `QueueOptions`, `Promise<T>`).
  - Worker execution loop managing active `runningCount` against configured `concurrency`.
  - Deterministic state machine tracking task status (`pending` | `processing` | `completed` | `failed`).
  - Pass-by-reference execution model preserving V8 Heap efficiency.

- [ ] **Stage 2: Resilience, Retries & Dead-Letter Queue (DLQ)**
- [ ] **Stage 3: Intake Strategies & Backpressure Control**
- [ ] **Stage 4: Benchmarking, Memory Profiling & $O(1)$ Queue Optimization**

---
