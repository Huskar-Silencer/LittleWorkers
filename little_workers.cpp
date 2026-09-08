#include "little_workers.h"

#include <stdexcept>

namespace littleworkers {

void LittleWorkers::TaskGroup::get() const {
  std::unique_lock lock(state_->mutex);
  state_->cv.wait(lock, [this] { return state_->remaining.load() == 0; });
  if (state_->first_exception) std::rethrow_exception(state_->first_exception);
}

LittleWorkers::LittleWorkers(const Options& options)
    : state_(std::make_shared<PoolState>()) {
  if (options.max_thread_size == 0 ||
      options.core_thread_size > options.max_thread_size) {
    throw std::invalid_argument(
        "LittleWorkers: core_thread_size must be in [0, max_thread_size] and "
        "max_thread_size must be > 0");
  }
  if (options.allow_core_thread_timeout &&
      (options.core_thread_size == 0 ||
       options.keep_alive <= std::chrono::milliseconds(0))) {
    throw std::invalid_argument(
        "LittleWorkers: allow_core_thread_timeout requires core_thread_size > "
        "0 and keep_alive > 0");
  }
  state_->core_thread_size = options.core_thread_size;
  state_->max_thread_size = options.max_thread_size;
  state_->keep_alive = options.keep_alive;
  state_->queue_capacity = options.queue_capacity;
  state_->reject_policy = options.reject_policy;
  state_->allow_core_thread_timeout = options.allow_core_thread_timeout;
}

void LittleWorkers::execute(TaskFuncType task) const {
  bool create_core = false;
  bool create_noncore = false;
  bool should_reject = false;
  bool enqueued = false;
  {
    std::lock_guard lock(state_->mutex);
    if (state_->is_stop) return;
    if (state_->thread_size.load() < state_->core_thread_size) {
      state_->thread_size.fetch_add(1);
      create_core = true;
    } else if (state_->queue_capacity == 0 ||
               state_->task_queue.size() < state_->queue_capacity) {
      state_->task_queue.emplace_back(std::move(task));
      enqueued = true;
    } else if (state_->thread_size.load() < state_->max_thread_size) {
      state_->thread_size.fetch_add(1);
      create_noncore = true;
    } else {
      should_reject = true;
    }
  }

  if (enqueued) {
    state_->cond.notify_one();
    return;
  }

  if (create_core) {
    startWorker(std::move(task), true);
  } else if (create_noncore) {
    startWorker(std::move(task), false);
  } else if (should_reject) {
    reject(std::move(task));
  }
}

void LittleWorkers::startWorker(TaskFuncType first_task,
                                const bool is_core) const {
  const auto thread_ptr = std::make_shared<std::thread>();
  ThreadRunParam param{.state = state_,
                       .thread_ptr = thread_ptr,
                       .first_task = std::move(first_task),
                       .is_core = is_core};
  try {
    *thread_ptr = std::thread(threadRun, std::move(param));
  } catch (...) {
    state_->thread_size.fetch_sub(1);
    throw;
  }
}

void LittleWorkers::threadRun(ThreadRunParam param) {
  const PoolStatePtrType state = std::move(param.state);
  const bool is_core = param.is_core;

  TaskFuncType task = std::move(param.first_task);
  if (!task) task = getTaskFromQueue(state, is_core);
  while (task) {
    try {
      task();
    } catch (...) {
    }
    state->completed_tasks.fetch_add(1, std::memory_order_relaxed);
    task = getTaskFromQueue(state, is_core);
  }

  state->thread_size.fetch_sub(1);
  state->stop_cond.notify_all();
  param.thread_ptr->detach();
}

LittleWorkers::TaskFuncType LittleWorkers::getTaskFromQueue(
    const std::shared_ptr<PoolState>& state, const bool is_core) {
  std::unique_lock lock(state->mutex);
  for (;;) {
    if (state->is_stop && state->task_queue.empty()) return nullptr;
    if (!state->task_queue.empty()) {
      TaskFuncType task = std::move(state->task_queue.front());
      state->task_queue.pop_front();
      return task;
    }
    if (state->allow_core_thread_timeout || !is_core) {
      if (state->cond.wait_for(lock, state->keep_alive) ==
          std::cv_status::timeout) {
        const bool can_shrink =
            state->thread_size.load() > state->core_thread_size ||
            state->allow_core_thread_timeout;
        const bool safe =
            state->thread_size.load() > 1 || state->task_queue.empty();
        if (can_shrink && safe) return nullptr;
      }
    } else {
      state->cond.wait(lock);
    }
  }
}

void LittleWorkers::reject(TaskFuncType task) const {
  switch (state_->reject_policy) {
    case RejectPolicy::kDiscard:
      return;
    case RejectPolicy::kCallerRuns:
      task();
      return;
    case RejectPolicy::kDiscardOldest: {
      bool enqueued = false;
      {
        std::lock_guard lock(state_->mutex);
        if (!state_->task_queue.empty()) {
          state_->task_queue.pop_front();
          state_->task_queue.push_back(std::move(task));
          enqueued = true;
        }
      }
      if (enqueued) state_->cond.notify_one();
      return;
    }
    case RejectPolicy::kAbort:
    default:
      throw std::runtime_error("LittleWorkers: task rejected by pool");
  }
}

void LittleWorkers::Stop() const {
  {
    std::lock_guard lock(state_->mutex);
    if (state_->is_stop) return;
    state_->is_stop = true;
  }
  state_->cond.notify_all();
}

LittleWorkers::TaskFuncVecType LittleWorkers::StopNow() const {
  TaskFuncVecType pending;
  {
    std::lock_guard lock(state_->mutex);
    if (state_->is_stop) return pending;
    state_->is_stop = true;
    while (!state_->task_queue.empty()) {
      pending.emplace_back(std::move(state_->task_queue.front()));
      state_->task_queue.pop_front();
    }
  }
  state_->cond.notify_all();
  return pending;
}

void LittleWorkers::WaitAll() const {
  std::unique_lock lock(state_->mutex);
  state_->stop_cond.wait(lock,
                         [this] { return state_->thread_size.load() == 0; });
}

bool LittleWorkers::AwaitTermination(
    const std::chrono::milliseconds timeout) const {
  std::unique_lock lock(state_->mutex);
  return state_->stop_cond.wait_for(
      lock, timeout, [this] { return state_->thread_size.load() == 0; });
}

void LittleWorkers::SetAllowCoreThreadTimeOut(const bool value) const {
  std::lock_guard lock(state_->mutex);
  if (value && (state_->core_thread_size == 0 ||
                state_->keep_alive <= std::chrono::milliseconds(0))) {
    throw std::invalid_argument(
        "LittleWorkers: allow_core_thread_timeout requires core_thread_size > "
        "0 and keep_alive > 0");
  }
  state_->allow_core_thread_timeout = value;
  state_->cond.notify_all();
}

LittleWorkers::TaskGroup LittleWorkers::SubmitGroup(
    TaskFuncVecType tasks) const {
  auto state = std::make_shared<GroupState>();
  state->remaining = static_cast<uint32_t>(tasks.size());
  for (auto& task : tasks) {
    submitIntoGroup(state, std::move(task));
  }
  return TaskGroup(std::move(state));
}

}  // namespace littleworkers
