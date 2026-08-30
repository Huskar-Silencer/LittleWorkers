#include "little_workers.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <future>
#include <mutex>
#include <thread>
#include <vector>

using namespace littleworkers;
using namespace std::chrono_literals;

int main() {
  LittleWorkers::Options opt;
  opt.core_thread_size = 4;
  opt.max_thread_size = 8;
  opt.queue_capacity = 0;  // 無界隊列: 突發請求不丟失
  LittleWorkers pool(opt);

  constexpr int kProducers = 4;
  constexpr int kPerProducer = 100;
  constexpr int kTotal = kProducers * kPerProducer;

  std::atomic<int> in_flight{0}, max_in_flight{0};
  std::vector<std::future<int>> responses;
  responses.reserve(kTotal);
  std::mutex responses_mutex;

  std::vector<std::thread> producers;
  for (int p = 0; p < kProducers; ++p) {
    producers.emplace_back([&, p] {
      for (int i = 0; i < kPerProducer; ++i) {
        const int request_id = p * kPerProducer + i;
        auto fut = pool.Submit([request_id, &in_flight, &max_in_flight] {
          int cur = in_flight.fetch_add(1) + 1;
          int mx = max_in_flight.load();
          while (cur > mx && !max_in_flight.compare_exchange_weak(mx, cur)) {
          }
          std::this_thread::sleep_for(1ms);
          in_flight.fetch_sub(1);
          return request_id;
        });
        std::lock_guard<std::mutex> lock(responses_mutex);
        responses.push_back(std::move(fut));
      }
    });
  }
  for (auto& t : producers) {
    t.join();
  }

  assert(static_cast<int>(responses.size()) == kTotal);
  std::vector<bool> seen(kTotal, false);
  for (auto& f : responses) {
    int id = f.get();
    assert(id >= 0 && id < kTotal);
    assert(!seen[id]);
    seen[id] = true;
  }
  std::printf("PASS http: responses=%zu unique=%d max_in_flight=%d\n",
              responses.size(), kTotal, max_in_flight.load());
  return 0;
}
