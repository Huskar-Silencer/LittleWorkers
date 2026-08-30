#include "little_workers.h"

#include <atomic>
#include <cassert>
#include <cstdio>
#include <future>

using namespace littleworkers;

int main() {
  {
    LittleWorkers::Options opt;
    opt.core_thread_size = 1;
    LittleWorkers pool(opt);

    std::atomic<int> counter{0};
    for (int i = 0; i < 100; ++i) {
      pool.Post([&counter] { counter.fetch_add(1); });
    }
    pool.Post([](int a, int b) { assert(a + b == 5); }, 2, 3);

    auto sentinel = pool.Submit([] { return true; });
    assert(sentinel.get());
    assert(counter.load() == 100);
    std::printf("PASS post: fire-and-forget=%d\n", counter.load());
  }
  return 0;
}
