#include "little_workers.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <stdexcept>
#include <thread>

using namespace littleworkers;
using namespace std::chrono_literals;

int main() {
  // 1. get() waits until every task in the group completes
  {
    LittleWorkers pool;
    std::atomic<int> counter{0};
    auto group = pool.SubmitGroup(
        [&] {
          std::this_thread::sleep_for(20ms);
          counter.fetch_add(1);
        },
        [&] { counter.fetch_add(1); },
        [&] {
          std::this_thread::sleep_for(5ms);
          counter.fetch_add(1);
        });
    group.get();
    assert(counter.load() == 3);
    std::printf("PASS group: completed=%d\n", counter.load());
  }

  // 2. first exception from a group task is rethrown by get()
  {
    LittleWorkers pool;
    auto group = pool.SubmitGroup([] { throw std::runtime_error("boom"); },
                                  [] {});
    bool threw = false;
    try {
      group.get();
    } catch (const std::runtime_error&) {
      threw = true;
    }
    assert(threw);
    std::printf("PASS group: exception propagated\n");
  }

  // 3. empty group completes immediately
  {
    LittleWorkers pool;
    auto group = pool.SubmitGroup();
    group.get();
    std::printf("PASS group: empty\n");
  }

  // 4. group with heterogeneous return types (results ignored)
  {
    LittleWorkers pool;
    auto group = pool.SubmitGroup([] { return 1 + 2; },
                                  [] { return std::string("hi"); },
                                  [] { return 3.14; });
    group.get();
    std::printf("PASS group: heterogeneous\n");
  }

  // 5. multi-thread pool: group across concurrent workers
  {
    LittleWorkers::Options opt;
    opt.core_thread_size = 4;
    LittleWorkers pool(opt);
    std::atomic<int> counter{0};
    auto group = pool.SubmitGroup(
        [&] { std::this_thread::sleep_for(5ms); counter.fetch_add(1); },
        [&] { std::this_thread::sleep_for(5ms); counter.fetch_add(1); },
        [&] { std::this_thread::sleep_for(5ms); counter.fetch_add(1); },
        [&] { std::this_thread::sleep_for(5ms); counter.fetch_add(1); });
    group.get();
    assert(counter.load() == 4);
    std::printf("PASS group: concurrent completed=%d\n", counter.load());
  }
  return 0;
}
