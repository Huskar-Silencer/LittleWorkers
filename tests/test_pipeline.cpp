#include "little_workers.h"

#include <cassert>
#include <cstdio>
#include <future>
#include <vector>

using namespace littleworkers;

int main() {
  LittleWorkers::Options opt;
  opt.core_thread_size = 4;
  opt.max_thread_size = 8;
  LittleWorkers pool(opt);

  constexpr int kImages = 32;

  std::vector<std::future<int>> decoded;
  for (int i = 0; i < kImages; ++i) {
    decoded.push_back(pool.Submit([i] { return i * 2; }));
  }
  std::vector<int> decoded_data(kImages);
  for (int i = 0; i < kImages; ++i) {
    decoded_data[i] = decoded[i].get();
  }

  std::vector<std::future<int>> scaled;
  for (int i = 0; i < kImages; ++i) {
    scaled.push_back(pool.Submit([v = decoded_data[i]] { return v + 1; }));
  }
  std::vector<int> scaled_data(kImages);
  for (int i = 0; i < kImages; ++i) {
    scaled_data[i] = scaled[i].get();
  }

  std::vector<std::future<int>> filtered;
  for (int i = 0; i < kImages; ++i) {
    filtered.push_back(pool.Submit([v = scaled_data[i]] { return v * 3; }));
  }
  for (int i = 0; i < kImages; ++i) {
    assert(filtered[i].get() == (i * 2 + 1) * 3);
  }

  std::printf("PASS pipeline: images=%d stages=3\n", kImages);
  return 0;
}
