#ifndef LITTLEWORKERS_LIBRARY_H
#define LITTLEWORKERS_LIBRARY_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace littleworkers {

class LittleWorkers {
  using ThreadPtrType = std::shared_ptr<std::thread>;
  using TaskFuncType = std::function<void()>;
  using TaskQueueType = std::deque<TaskFuncType>;
  using TaskVectorType = std::vector<TaskFuncType>;
  using PoolMutexType = std::mutex;
  using PoolCondType = std::condition_variable;
  using AtomicUInt32Type = std::atomic<uint32_t>;
  using ChronoMilliSecType = std::chrono::milliseconds;

  struct GroupState;

  static constexpr uint32_t kDefaultMaxThreadSize = 4;
  static constexpr uint32_t kDefaultCoreThreadSize = 1;
  static constexpr uint32_t kDefaultTaskQueueSize = 512;
  static constexpr uint32_t kDefaultTimeoutMs = 60 * 1000;

 public:
  enum class RejectPolicy {
    kAbort,
    kDiscard,
    kCallerRuns,
    kDiscardOldest,
  };

  struct Options {
    uint32_t core_thread_size = kDefaultCoreThreadSize;
    uint32_t max_thread_size = kDefaultMaxThreadSize;
    ChronoMilliSecType keep_alive = ChronoMilliSecType(kDefaultTimeoutMs);
    size_t queue_capacity = kDefaultTaskQueueSize;
    RejectPolicy reject_policy = RejectPolicy::kAbort;
    bool allow_core_thread_timeout = false;
  };

  class TaskGroup {
   public:
    explicit TaskGroup(std::shared_ptr<GroupState> state)
        : state_(std::move(state)) {}
    void get() const;

   private:
    std::shared_ptr<GroupState> state_;
  };

 private:
  struct PoolState {
    TaskQueueType task_queue;
    PoolMutexType mutex;
    PoolCondType cond;
    PoolCondType stop_cond;
    AtomicUInt32Type thread_size{0};
    bool is_stop = false;

    uint32_t core_thread_size;
    uint32_t max_thread_size;
    ChronoMilliSecType keep_alive;
    size_t queue_capacity;
    RejectPolicy reject_policy;
    bool allow_core_thread_timeout = false;
  };

  using PoolStatePtrType = std::shared_ptr<PoolState>;

  struct GroupState {
    PoolMutexType mutex;
    PoolCondType cv;
    AtomicUInt32Type remaining{0};
    std::exception_ptr first_exception;
  };

  struct ThreadRunParam {
    PoolStatePtrType state;
    ThreadPtrType thread_ptr;
    TaskFuncType first_task;
    bool is_core = true;
  };

  PoolStatePtrType state_;

  void execute(TaskFuncType task) const;
  void startWorker(TaskFuncType first_task, bool is_core) const;
  void reject(TaskFuncType task) const;
  static TaskFuncType getTaskFromQueue(const PoolStatePtrType& state,
                                       bool is_core);
  static void threadRun(ThreadRunParam param);

  template <typename F>
  void submitIntoGroup(const std::shared_ptr<GroupState>& state,
                       F&& func) const {
    using DecayedF = std::decay_t<F>;
    TaskFuncType task = [state,
                         func = DecayedF(std::forward<F>(func))]() mutable {
      try {
        func();
      } catch (...) {
        std::lock_guard lock(state->mutex);
        if (!state->first_exception)
          state->first_exception = std::current_exception();
      }
      if (state->remaining.fetch_sub(1) == 1) state->cv.notify_all();
    };
    execute(std::move(task));
  }

 public:
  LittleWorkers() : LittleWorkers(Options()) {}
  explicit LittleWorkers(const Options& options);
  LittleWorkers(const LittleWorkers&) = delete;
  LittleWorkers& operator=(const LittleWorkers&) = delete;

  void Stop() const;
  void WaitAll() const;
  void SetAllowCoreThreadTimeOut(bool value) const;
  [[nodiscard]] TaskVectorType StopNow() const;

  [[nodiscard]] bool IsStopped() const {
    std::lock_guard lock(state_->mutex);
    return state_->is_stop;
  }

  [[nodiscard]] uint32_t ThreadSize() const {
    return state_->thread_size.load();
  }

  ~LittleWorkers() {
    Stop();
    WaitAll();
  }

  template <typename F, typename... Args>
  auto Submit(F&& func, Args&&... args)
      -> std::future<std::invoke_result_t<F, Args...>> {
    using ReturnType = std::invoke_result_t<F, Args...>;
    using TaskWrapper = std::packaged_task<ReturnType()>;
    auto task = std::make_shared<TaskWrapper>(
        std::bind(std::forward<F>(func), std::forward<Args>(args)...));
    std::future<ReturnType> result = task->get_future();
    TaskFuncType wrapper = [task]() mutable { (*task)(); };
    execute(std::move(wrapper));
    return result;
  }

  template <typename F, typename... Args>
  void Post(F&& func, Args&&... args) {
    TaskFuncType task =
        [bound = std::bind(std::forward<F>(func),
                           std::forward<Args>(args)...)]() mutable { bound(); };
    execute(std::move(task));
  }

  template <typename... Fs>
  TaskGroup SubmitGroup(Fs&&... funcs) {
    constexpr size_t kCount = sizeof...(Fs);
    auto state = std::make_shared<GroupState>();
    state->remaining = static_cast<uint32_t>(kCount);
    (submitIntoGroup(state, std::forward<Fs>(funcs)), ...);
    return TaskGroup(std::move(state));
  }
};
}  // namespace littleworkers

#endif  // LITTLEWORKERS_LIBRARY_H
