#include "little_workers.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <future>
#include <vector>

using namespace littleworkers;
using namespace std::chrono_literals;

struct Order {
  int id;
  int price;
  int count;
};

int main() {
  LittleWorkers::Options opt;
  opt.core_thread_size = 2;
  opt.max_thread_size = 2;
  LittleWorkers pool(opt);

  const std::vector<Order> orders = {
      {1, 100, 2}, {2, 50, 4}, {3, 30, 10}, {4, 200, 1}, {5, 10, 20}};

  long long expected_total = 0;
  for (const auto& o : orders) {
    expected_total += static_cast<long long>(o.price) * o.count;
  }

  std::atomic<int> in_flight{0}, peak{0};
  std::vector<std::future<int>> futures;
  for (const auto& o : orders) {
    futures.push_back(pool.Submit([o, &in_flight, &peak] {
      int cur = in_flight.fetch_add(1) + 1;
      int p = peak.load();
      while (cur > p && !peak.compare_exchange_weak(p, cur)) {
      }
      std::this_thread::sleep_for(2ms);
      in_flight.fetch_sub(1);
      return o.price * o.count;
    }));
  }

  long long total = 0;
  for (auto& f : futures) {
    total += f.get();
  }

  assert(total == expected_total);
  assert(peak.load() == 2);
  std::printf("PASS orders: processed=%zu total=%lld peak_concurrency=%d\n",
              orders.size(), total, peak.load());
  return 0;
}
