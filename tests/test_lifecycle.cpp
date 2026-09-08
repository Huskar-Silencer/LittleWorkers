#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <functional>
#include <thread>
#include <vector>

#include "little_workers.h"

using namespace littleworkers;
using namespace std::chrono_literals;

int main() {
  // lifecycle: AwaitTermination / QueueSize / CompletedTaskCount
  {
    LittleWorkers::Options opt;
    opt.core_thread_size = 1;
    LittleWorkers pool(opt);

    std::atomic<int> ran{0};
    for (int i = 0; i < 8; ++i) {
      pool.Submit([&] {
        std::this_thread::sleep_for(5ms);
        ran.fetch_add(1);
      });
    }
    pool.Stop();
    assert(pool.AwaitTermination(2s));
    assert(ran.load() == 8);
    assert(pool.CompletedTaskCount() == 8);
    assert(pool.QueueSize() == 0);
    std::printf("PASS lifecycle: completed=%llu queue=%zu\n",
                static_cast<unsigned long long>(pool.CompletedTaskCount()),
                pool.QueueSize());
  }

  // AwaitTermination returns false when workers never stop
  {
    LittleWorkers pool;
    pool.Submit([] {});
    auto ok = pool.AwaitTermination(100ms);
    assert(ok == false);
    pool.Stop();
    pool.WaitAll();
    std::printf("PASS await timeout: still_running=%d\n", !ok);
  }

  // SubmitGroup(container) built in a loop
  {
    LittleWorkers::Options opt;
    opt.core_thread_size = 4;
    LittleWorkers pool(opt);

    std::vector<int> results(16, 0);
    std::vector<std::function<void()>> tasks;
    for (int i = 0; i < 16; ++i) {
      tasks.emplace_back([&results, i] { results[i] = i * i; });
    }
    auto group = pool.SubmitGroup(std::move(tasks));
    group.get();
    for (int i = 0; i < 16; ++i) {
      assert(results[i] == i * i);
    }
    std::printf("PASS group container: tasks=16\n");
  }

  std::printf("all lifecycle tests passed\n");
  return 0;
}
