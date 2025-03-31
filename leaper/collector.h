#pragma once

#include <atomic>
#include <chrono>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class LeaperCollector {
 public:
  LeaperCollector(size_t num_ranges, uint64_t interval_sec, const std::string& output_path);
  ~LeaperCollector();

  void RecordKeyAccess(const Slice& user_key);
  void Stop();

 private:
  void DumpStatsToCSV();                  
  void BackgroundThread();              
  size_t KeyToRangeID(const Slice& key); 

  size_t num_ranges_;
  std::vector<std::atomic<uint32_t>> counters_;
  std::thread background_thread_;
  std::atomic<bool> stop_flag_;
  std::mutex file_mutex_;

  uint64_t interval_sec_;
  std::string output_path_;
};

extern LeaperCollector* leaper_read_collector;
extern LeaperCollector* leaper_write_collector;
}  
