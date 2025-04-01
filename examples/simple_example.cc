// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <iostream>
#include <random>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
std::string kDBPath = "/tmp/rocksdb_simple_example";
#endif

int main() {
  DB* db;
  Options options;
  std::cout << "simple_example runs." << std::endl;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // Put key-value
  s = db->Put(WriteOptions(), "1461201", "value");
  assert(s.ok());
  std::string value;
  // get value
  s = db->Get(ReadOptions(), "1461201", &value);
  assert(s.ok());
  assert(value == "value");

  // atomically apply a set of updates
  {
    WriteBatch batch;
    batch.Delete("1461201");
    batch.Put("1461202", value);
    s = db->Write(WriteOptions(), &batch);
  }

  s = db->Get(ReadOptions(), "1461201", &value);
  assert(s.IsNotFound());

  db->Get(ReadOptions(), "1461202", &value);
  assert(value == "value");

  {
    PinnableSlice pinnable_val;
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "1461202", &pinnable_val);
    assert(pinnable_val == "value");
  }

  {
    std::string string_val;
    // If it cannot pin the value, it copies the value to its internal buffer.
    // The intenral buffer could be set during construction.
    PinnableSlice pinnable_val(&string_val);
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "1461202", &pinnable_val);
    assert(pinnable_val == "value");
    // If the value is not pinned, the internal buffer must have the value.
    assert(pinnable_val.IsPinned() || string_val == "value");
  }

  PinnableSlice pinnable_val;
  s = db->Get(ReadOptions(), db->DefaultColumnFamily(), "1461201",
              &pinnable_val);
  assert(s.IsNotFound());
  // Reset PinnableSlice after each use and before each reuse
  pinnable_val.Reset();
  db->Get(ReadOptions(), db->DefaultColumnFamily(), "1461202", &pinnable_val);
  assert(pinnable_val == "value");
  pinnable_val.Reset();
  // The Slice pointed by pinnable_val is not valid after this point

  // ---------- 强化测试：成千上万次 Put / Get ----------
  std::default_random_engine rng(
      (unsigned)std::chrono::system_clock::now().time_since_epoch().count());
  std::uniform_int_distribution<int> dist(0, 19999);  // key0~key19999

  std::cout << "Starting mass Put..." << std::endl;

  for (int i = 0; i < 100000; ++i) {
    std::string key = "mass_key_" + std::to_string(i);
    std::string val = "val_" + std::to_string(i);
    s = db->Put(WriteOptions(), key, val);
    assert(s.ok());
  }

  std::cout << "Starting mass Get..." << std::endl;

  for (int i = 0; i < 100000; ++i) {
    std::string key = "mass_key_" + std::to_string(dist(rng));
    std::string val;
    s = db->Get(ReadOptions(), key, &val);
    if (!s.ok() && !s.IsNotFound()) {
      std::cerr << "Get failed for key: " << key << std::endl;
    }
  }

  // ---------- 等待 Leaper 后台线程写出 CSV ----------
  std::this_thread::sleep_for(std::chrono::seconds(6));

  delete db;
  std::cout << "simple_example finishes." << std::endl;
  return 0;
}
