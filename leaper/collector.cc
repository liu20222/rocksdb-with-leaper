#include "leaper/collector.h"

#include <cctype>  // for isdigit
#include <ctime>
#include <functional>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
namespace ROCKSDB_NAMESPACE {
LeaperCollector* leaper_read_collector = nullptr;
LeaperCollector* leaper_write_collector = nullptr;

// LeaperCollector::LeaperCollector(size_t num_ranges, uint64_t interval_sec,
//                                  const std::string& output_path)
//     : num_ranges_(num_ranges),
//       counters_(num_ranges),
//       stop_flag_(false),
//       interval_sec_(interval_sec),
//       output_path_(output_path) {
//   for (size_t i = 0; i < num_ranges_; ++i) {
//     counters_[i] = 0;
//   }
//   background_thread_ =
//       std::thread(&LeaperCollector::BackgroundThread,
//                   this);  // GPT write BackgroundThread() out of it.
// }
LeaperCollector::LeaperCollector(size_t num_ranges, uint64_t interval_sec,
                                 const std::string& output_path)
    : num_ranges_(num_ranges),
      counters_(num_ranges),
      stop_flag_(false),
      init_mutexes_(
          num_ranges),  // For double-checked lock and lazy initialization
      interval_sec_(interval_sec),
      output_path_(output_path) {
  background_thread_ = std::thread(&LeaperCollector::BackgroundThread, this);
}

LeaperCollector::~LeaperCollector() {
  Stop();
}  // GPT write Stop() out of it. Benifit:可以提前调用，不等析构时才停

void LeaperCollector::Stop() {
  stop_flag_.store(true);
  if (background_thread_.joinable()) {
    background_thread_.join();
  }
}

// void LeaperCollector::RecordKeyAccess(const Slice& user_key) {
//     // std::cout << "[Leaper] RecordKeyAccess called." << std::endl;
//     size_t id = KeyToRangeID(user_key);
//     counters_[id].fetch_add(1, std::memory_order_relaxed);
// }
// void LeaperCollector::RecordKeyAccess(const Slice& user_key) {
//   size_t id = KeyToRangeID(user_key);
//   std::cout << "[Leaper] RecordKeyAccess: key=" << user_key.ToString()
//             << ", range_id=" << id << std::endl;
//   if (id < counters_.size()) {
//     counters_[id].fetch_add(1, std::memory_order_relaxed);
//   } else {
//     std::cerr << "[Leaper] ERROR in Collector.(Range ID out of range)"
//               << std::endl;
//   }
// }
void LeaperCollector::RecordKeyAccess(const Slice& user_key) {
  size_t id = KeyToRangeID(user_key);
  if (id >= counters_.size()) {
    std::cerr << "[Leaper] ERROR in collector's RecordKeyAccess()."
              << std::endl;
    return;
  }

  // Double-checked locking + lazy init
  if (!counters_[id]) {
    std::lock_guard<std::mutex> lg(init_mutexes_[id]);
    if (!counters_[id]) {
      counters_[id] = std::make_unique<std::atomic<uint64_t>>(0);
    }
  }

  counters_[id]->fetch_add(1, std::memory_order_relaxed);
}

// size_t LeaperCollector::KeyToRangeID(const Slice& key) {
//   std::hash<std::string> hasher;
//   uint64_t hash = hasher(key.ToString());
//   return hash % num_ranges_;
// }
// size_t LeaperCollector::KeyToRangeID(const Slice& key) {
//   uint64_t int_key = std::stoull(key.ToString());
//   return int_key / 10000;
// }

size_t LeaperCollector::KeyToRangeID(const Slice& key) {
  std::string numeric_part;

  for (size_t i = 0; i < key.size(); ++i) {
    if (std::isdigit(key[i])) {
      numeric_part += key[i];
    }
  }

  if (numeric_part.empty()) {
    return 0;
  }

  uint64_t int_key = std::stoull(numeric_part);

  const uint64_t range_width = 5;
  return int_key / range_width;
}

// void LeaperCollector::DumpStatsToCSV() {
//   std::ostringstream oss;

//   auto now = std::chrono::system_clock::now();
//   std::time_t t_now = std::chrono::system_clock::to_time_t(now);
//   oss << std::put_time(std::localtime(&t_now), "%F %T");

//   for (size_t i = 0; i < num_ranges_; ++i) {
//     oss << "," << counters_[i].load(std::memory_order_relaxed);
//     counters_[i] = 0;
//   }

//   oss << "\n";

//   std::lock_guard<std::mutex> lg(file_mutex_);
//   std::ofstream out(output_path_, std::ios::app);
//   if (out.is_open()) {
//     out << oss.str();
//   }
// }

// For double-checked lock and lazy initialization
void LeaperCollector::DumpStatsToCSV() {
  std::ostringstream oss;

  auto now = std::chrono::system_clock::now();
  std::time_t t_now = std::chrono::system_clock::to_time_t(now);
  oss << std::put_time(std::localtime(&t_now), "%F %T");

  for (size_t i = 0; i < num_ranges_; ++i) {
    if (counters_[i]) {
      oss << "," << counters_[i]->load(std::memory_order_relaxed);
      counters_[i]->store(0, std::memory_order_relaxed);
    } else {
      oss << ",0";
    }
  }

  oss << "\n";

  std::lock_guard<std::mutex> lg(file_mutex_);
  std::ofstream out(output_path_, std::ios::app);
  if (out.is_open()) {
    out << oss.str();
  }
}

void LeaperCollector::BackgroundThread() {
  while (!stop_flag_.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(interval_sec_));
    DumpStatsToCSV();
  }
}

}  // namespace ROCKSDB_NAMESPACE
