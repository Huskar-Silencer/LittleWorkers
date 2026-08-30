#include "little_workers.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <future>
#include <thread>
#include <vector>

using namespace littleworkers;
using namespace std::chrono_literals;

int main() {
  {
    LittleWorkers::Options opt;
    opt.core_thread_size = 2;
    opt.max_thread_size = 4;
    opt.queue_capacity = 2;
    opt.keep_alive = 200ms;
    LittleWorkers pool(opt);

    std::atomic<int> in_flight{0}, peak{0};
    std::vector<std::future<void>> futures;
    for (int i = 0; i < 6; ++i) {
      futures.push_back(pool.Submit([&in_flight, &peak] {
        int cur = in_flight.fetch_add(1) + 1;
        int p = peak.load();
        while (cur > p && !peak.compare_exchange_weak(p, cur)) {
        }
        std::this_thread::sleep_for(5ms);
        in_flight.fetch_sub(1);
      }));
    }
    for (auto& f : futures) {
      f.get();
    }
    assert(peak.load() == 4);
    std::this_thread::sleep_for(500ms);
    assert(pool.ThreadSize() == 2);
    std::printf("PASS scaling: expanded_to=%d shrunk_to=%u\n", peak.load(),
                pool.ThreadSize());
  }

  {
    LittleWorkers::Options opt;
    opt.core_thread_size = 1;
    opt.max_thread_size = 1;
    opt.queue_capacity = 1;
    opt.reject_policy = LittleWorkers::RejectPolicy::kDiscardOldest;
    LittleWorkers pool(opt);

    std::atomic<bool> oldest_done{false}, newest_done{false};
    auto blocker = pool.Submit([] { std::this_thread::sleep_for(200ms); });
    (void)blocker;
    pool.Submit([&oldest_done] { oldest_done = true; });
    pool.Submit([&newest_done] { newest_done = true; });

    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!newest_done.load() && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(5ms);
    }
    assert(!oldest_done.load());
    assert(newest_done.load());
    std::printf("PASS discard-oldest: oldest=%d newest=%d\n",
                static_cast<int>(oldest_done.load()),
                static_cast<int>(newest_done.load()));
  }
  return 0;
}
