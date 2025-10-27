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

#include "VeloxHashTableRegistry.h"

namespace gluten {

VeloxHashTableRegistry& VeloxHashTableRegistry::instance() {
  static VeloxHashTableRegistry registry;
  return registry;
}

std::shared_ptr<facebook::velox::exec::BaseHashTable> VeloxHashTableRegistry::registerSerialized(
    const std::string& id,
    const std::string& serialized,
    facebook::velox::memory::MemoryPool* pool) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto& entry = tables_[id];
  if (!entry.table) {
    entry.table = facebook::velox::exec::BaseHashTable::deserialize(serialized, pool);
  }
  ++entry.refCount;
  return entry.table;
}

std::shared_ptr<facebook::velox::exec::BaseHashTable> VeloxHashTableRegistry::get(const std::string& id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = tables_.find(id);
  if (it == tables_.end()) {
    return nullptr;
  }
  return it->second.table;
}

void VeloxHashTableRegistry::unregister(const std::string& id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = tables_.find(id);
  if (it == tables_.end()) {
    return;
  }
  if (it->second.refCount > 1) {
    --it->second.refCount;
    return;
  }
  tables_.erase(it);
}

} // namespace gluten
