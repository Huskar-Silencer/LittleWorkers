# LittleWorkers

LittleWorkers is a lightweight C++17 thread pool library, designed to provide a simple, efficient and fully functional task scheduling capability for C++ projects.

## Features

- `Submit(F&&, Args&&...)` returns a `std::future` carrying the task result or exception.
- Configurable core / maximum pool size, idle keep-alive, bounded task queue, and rejection policy.
- Core threads stay alive; non-core threads are reaped after `keep_alive`.
- Optional `allowCoreThreadTimeOut` so idle core threads also retire (Java-compatible behavior).
- `Stop` / `StopNow` / `WaitAll` for graceful or immediate shutdown.
- Tasks are executed inside `try/catch`; exceptions never escape the worker thread and are always surfaced through the
  returned `std::future`.
- RAII: the destructor stops the pool and waits for all workers to exit.

## Building

```sh
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

## Usage

```cpp
#include "little_workers.h"
#include <cstdio>

int main() {
  littleworkers::LittleWorkers::Options opt;
  opt.core_thread_size = 4;
  opt.max_thread_size = 8;
  opt.queue_capacity = 256;
  littleworkers::LittleWorkers pool(opt);

  auto sum = pool.Submit([](int a, int b) { return a + b; }, 2, 3);
  auto msg = pool.Submit([]() { return "hello"; });

  std::printf("%d %s\n", sum.get(), msg.get());

  pool.Post([] { std::puts("fire and forget"); });
  pool.Stop();
  pool.WaitAll();
  return 0;
}
```

Submit several tasks in one call and wait for all of them:

```cpp
#include "little_workers.h"
#include <cstdio>

int main() {
  littleworkers::LittleWorkers pool;

  auto group = pool.SubmitGroup(
      [] { std::puts("task A"); },
      [] { std::puts("task B"); },
      [] { std::puts("task C"); });
  group.get();  // waits until all tasks complete

  return 0;
}
```

### API

| Method / Type                                       | Description                                                                               |
|-----------------------------------------------------|-------------------------------------------------------------------------------------------|
| `LittleWorkers()` / `LittleWorkers(const Options&)` | Create a pool.                                                                            |
| `Submit(f, args...)`                                | Enqueue a task, returns `std::future<Result>`.                                            |
| `Post(f, args...)`                                  | Enqueue a fire-and-forget task; no `std::future` is created (Java's `execute(Runnable)`). |
| `SubmitGroup(fs...)`                                | Enqueue several tasks in one call; returns a `TaskGroup`.                                 |
| `TaskGroup::get()`                                  | Block until every task in the group completes; rethrows the first exception, if any.      |
| `Stop()`                                            | No new tasks accepted; queued tasks still run to completion.                              |
| `StopNow()`                                         | Stop and return the still-queued tasks (running tasks finish).                            |
| `WaitAll()`                                         | Block until every worker has exited. Only returns after `Stop()`/`StopNow()`.             |
| `SetAllowCoreThreadTimeOut(bool)`                   | Enable/disable core-thread timeout at runtime.                                            |
| `ThreadSize()`                                      | Current number of live worker threads.                                                    |
| `IsStopped()`                                       | Whether the pool has been stopped.                                                        |

### `Options`

| Field                       | Default  | Meaning                                                          |
|-----------------------------|----------|------------------------------------------------------------------|
| `core_thread_size`          | 1        | Threads kept alive when idle.                                    |
| `max_thread_size`           | 4        | Upper bound on concurrent workers.                               |
| `keep_alive`                | 60s      | Idle lifetime of non-core threads.                               |
| `queue_capacity`            | 512      | Bounded task queue; `0` means unbounded.                         |
| `reject_policy`             | `kAbort` | Policy applied when the queue is full and the pool is saturated. |
| `allow_core_thread_timeout` | false    | If true, core threads also time out and can retire.              |

### `RejectPolicy`

| Policy           | Behavior                                               |
|------------------|--------------------------------------------------------|
| `kAbort`         | Throw `std::runtime_error` from `Submit`.              |
| `kDiscard`       | Silently drop the task.                                |
| `kCallerRuns`    | Run the task on the submitting thread.                 |
| `kDiscardOldest` | Drop the head of the queue, then enqueue the new task. |

## Task dispatch order

Both `Submit` and `Post` follow the Java `execute` path:

1. If `thread_count < core_thread_size`, start a new core worker.
2. Else, enqueue the task if the queue has room.
3. Else, if `thread_count < max_thread_size`, start a non-core worker.
4. Else, apply the rejection policy.

## Notes and caveats

- Shutdown semantics match Java: `Stop()` drains the queue, `StopNow()`
  discards queued work. Tasks already running are never interrupted.
- `Post` is fire-and-forget: since no `std::future` is created, exceptions thrown by a posted task are swallowed by the
  worker and cannot be observed. Use `Submit` if you need the result or the error.
- `SubmitGroup` / `TaskGroup::get()` rethrow the first exception thrown by any group task. With a discarding reject
  policy (`kDiscard`, `kDiscardOldest`), a dropped group task never runs and `get()` blocks forever — same as a
  never-completing `std::future` in Java.
- A single-threaded pool (`core_thread_size == 1`) will deadlock if a task submits another task and blocks on its
  future — same limitation as Java's
  `newSingleThreadExecutor`.
- With `core_thread_size == 0` and an unbounded queue, submitted tasks are queued but never picked up once all threads
  retire (a known Java gotcha). Prefer a bounded queue or `queue_capacity == 0` with `core_thread_size >= 1`.
- `allow_core_thread_timeout` requires `core_thread_size > 0` and
  `keep_alive > 0`, otherwise construction/setter throws
  `std::invalid_argument`.
- `WaitAll()` must not be called from inside a worker task of the same pool (it waits for all workers, including the
  caller).

## Tests

Business-scenario tests live under `tests/` and are registered with CTest:

- `test_orders` — batch order processing, total amount and concurrency cap.
- `test_http_server` — concurrent request handling, no lost/duplicate requests.
- `test_pipeline` — multi-stage image-processing pipeline.
- `test_scaling` — thread expansion/shrink and `kDiscardOldest` behavior.
- `test_post` — fire-and-forget `Post` tasks all run to completion.
- `test_group` — `SubmitGroup`/`TaskGroup::get()` waits for all tasks, exception propagation, empty and heterogeneous
  groups.
