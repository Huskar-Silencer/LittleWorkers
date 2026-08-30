#include "little_workers.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <stdexcept>
#include <thread>
#include <vector>

using namespace littleworkers;
using namespace std::chrono_literals;

// 場景 1: 並行健康檢查 — 每個服務自行容錯，group 只負責同步等待
static void test_health_check() {
  LittleWorkers::Options opt;
  opt.core_thread_size = 4;
  LittleWorkers pool(opt);

  std::vector<int> status(6, -1);
  auto group = pool.SubmitGroup(
      [&] {
        std::this_thread::sleep_for(5ms);
        status[0] = 200;
      },
      [&] {
        std::this_thread::sleep_for(3ms);
        status[1] = 200;
      },
      [&] {
        std::this_thread::sleep_for(4ms);
        status[2] = 200;
      },
      [&] {
        try {
          throw std::runtime_error("service down");
        } catch (...) {
          status[3] = 500;
        }
      },
      [&] { status[4] = 500; },
      [&] {
        std::this_thread::sleep_for(2ms);
        status[5] = 200;
      });
  group.get();

  int healthy = 0;
  for (int s : status) {
    healthy += (s == 200);
  }
  assert(healthy == 4);
  std::printf("PASS health_check: services=%zu healthy=%d\n", status.size(),
              healthy);
}

// 場景 2: 並行行情抓取後聚合 — 各槽位獨立寫入，get 後統一匯總
static void test_quote_aggregation() {
  LittleWorkers::Options opt;
  opt.core_thread_size = 4;
  LittleWorkers pool(opt);

  constexpr int kStocks = 6;
  std::vector<int> quotes(kStocks, 0);
  auto group = pool.SubmitGroup(
      [&] {
        std::this_thread::sleep_for(4ms);
        quotes[0] = 100;
      },
      [&] {
        std::this_thread::sleep_for(2ms);
        quotes[1] = 200;
      },
      [&] {
        std::this_thread::sleep_for(5ms);
        quotes[2] = 300;
      },
      [&] {
        std::this_thread::sleep_for(3ms);
        quotes[3] = 400;
      },
      [&] { quotes[4] = 500; },
      [&] {
        std::this_thread::sleep_for(1ms);
        quotes[5] = 600;
      });
  group.get();

  long long total = 0;
  for (int q : quotes) {
    total += q;
  }
  assert(total == 2100);
  std::printf("PASS quote_aggregation: stocks=%d total=%lld\n", kStocks, total);
}

// 場景 3: 分片統計 — 大數據集分成多片並行求和，get 後合併
static void test_sharded_sum() {
  LittleWorkers::Options opt;
  opt.core_thread_size = 4;
  LittleWorkers pool(opt);

  constexpr int kShards = 4;
  constexpr int kPerShard = 10000;
  std::vector<long long> partial(kShards, 0);

  auto shard = [&](int s) {
    long long sum = 0;
    for (int i = 0; i < kPerShard; ++i) {
      sum += static_cast<long long>(s) * kPerShard + i;
    }
    partial[s] = sum;
  };

  auto group = pool.SubmitGroup([&] { shard(0); }, [&] { shard(1); },
                                [&] { shard(2); }, [&] { shard(3); });
  group.get();

  long long total = 0;
  for (long long p : partial) {
    total += p;
  }
  long long expected = 0;
  for (int i = 0; i < kShards * kPerShard; ++i) {
    expected += i;
  }
  assert(total == expected);
  std::printf("PASS sharded_sum: total=%lld\n", total);
}

// 場景 4: 訂單結算遇失敗 — group.get() 拋出首個異常，池仍可繼續使用
static void test_settlement_with_failure() {
  LittleWorkers::Options opt;
  opt.core_thread_size = 4;
  LittleWorkers pool(opt);

  std::atomic<int> settled{0};
  auto group = pool.SubmitGroup(
      [&] { settled.fetch_add(1); },
      [&] { throw std::runtime_error("insufficient funds"); },
      [&] { settled.fetch_add(1); });

  bool threw = false;
  try {
    group.get();
  } catch (const std::runtime_error&) {
    threw = true;
  }
  assert(threw);
  assert(settled.load() == 2);

  auto f = pool.Submit([] { return 1; });
  assert(f.get() == 1);
  std::printf("PASS settlement_with_failure: settled=%d pool_alive=true\n",
              settled.load());
}

int main() {
  test_health_check();
  test_quote_aggregation();
  test_sharded_sum();
  test_settlement_with_failure();
  std::printf("all group business tests passed\n");
  return 0;
}
