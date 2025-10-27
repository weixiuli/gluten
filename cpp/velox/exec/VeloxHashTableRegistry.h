/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "velox/exec/HashTable.h"

namespace facebook::velox::memory {
class MemoryPool;
} // namespace facebook::velox::memory

namespace gluten {

class VeloxHashTableRegistry {
 public:
  static VeloxHashTableRegistry& instance();

  std::shared_ptr<facebook::velox::exec::BaseHashTable> registerSerialized(
      const std::string& id,
      const std::string& serialized,
      facebook::velox::memory::MemoryPool* pool);

  std::shared_ptr<facebook::velox::exec::BaseHashTable> get(const std::string& id);

  void unregister(const std::string& id);

 private:
  VeloxHashTableRegistry() = default;

  struct Entry {
    std::shared_ptr<facebook::velox::exec::BaseHashTable> table;
    size_t refCount{0};
  };

  std::mutex mutex_;
  std::unordered_map<std::string, Entry> tables_;
};

} // namespace gluten
