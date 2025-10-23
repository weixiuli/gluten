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

#include "exec/PrebuiltHashTable.h"

#include "velox/common/base/Exceptions.h"

namespace gluten {

std::unordered_map<std::string, std::shared_ptr<facebook::velox::exec::BaseHashTable>>
    VeloxPrebuiltHashTables::tables_{};
std::unordered_map<const facebook::velox::core::HashJoinNode*, VeloxPrebuiltHashTables::HashJoinAttachment>
    VeloxPrebuiltHashTables::joinAttachments_{};
std::mutex VeloxPrebuiltHashTables::mutex_;

void VeloxPrebuiltHashTables::cleanupExpiredJoinAttachmentsLocked() {
  for (auto it = joinAttachments_.begin(); it != joinAttachments_.end();) {
    if (it->second.node.expired()) {
      it = joinAttachments_.erase(it);
    } else {
      ++it;
    }
  }
}

void VeloxPrebuiltHashTables::registerSerialized(
    const std::string& id,
    const std::string& serialized,
    facebook::velox::memory::MemoryPool* pool) {
  auto table = facebook::velox::exec::BaseHashTable::deserialize(serialized, pool);
  if (!table) {
    VELOX_FAIL("Failed to deserialize prebuilt hash table for id {}", id);
  }

  std::lock_guard<std::mutex> guard(mutex_);
  tables_[id] = std::move(table);
}

std::shared_ptr<facebook::velox::exec::BaseHashTable> VeloxPrebuiltHashTables::get(const std::string& id) {
  std::lock_guard<std::mutex> guard(mutex_);
  cleanupExpiredJoinAttachmentsLocked();
  auto it = tables_.find(id);
  if (it == tables_.end()) {
    return nullptr;
  }
  return it->second;
}

void VeloxPrebuiltHashTables::erase(const std::string& id) {
  std::lock_guard<std::mutex> guard(mutex_);
  cleanupExpiredJoinAttachmentsLocked();
  tables_.erase(id);
  for (auto it = joinAttachments_.begin(); it != joinAttachments_.end();) {
    if (it->second.id == id || it->second.node.expired()) {
      it = joinAttachments_.erase(it);
    } else {
      ++it;
    }
  }
}

void VeloxPrebuiltHashTables::attachPrebuiltHashTable(
    const std::shared_ptr<const facebook::velox::core::HashJoinNode>& node,
    std::string id,
    std::shared_ptr<facebook::velox::exec::BaseHashTable> table) {
  VELOX_CHECK_NOT_NULL(node, "HashJoinNode must not be null when attaching prebuilt hash table");
  VELOX_CHECK_NOT_NULL(table, "Prebuilt hash table must not be null when attaching to HashJoinNode");

  std::lock_guard<std::mutex> guard(mutex_);
  cleanupExpiredJoinAttachmentsLocked();
  joinAttachments_[node.get()] = HashJoinAttachment{node, std::move(id), std::move(table)};
}

bool VeloxPrebuiltHashTables::hasPrebuiltHashTable(const facebook::velox::core::HashJoinNode& node) {
  std::lock_guard<std::mutex> guard(mutex_);
  cleanupExpiredJoinAttachmentsLocked();
  return joinAttachments_.find(&node) != joinAttachments_.end();
}

std::optional<std::string> VeloxPrebuiltHashTables::getPrebuiltHashTableId(
    const facebook::velox::core::HashJoinNode& node) {
  std::lock_guard<std::mutex> guard(mutex_);
  cleanupExpiredJoinAttachmentsLocked();
  auto it = joinAttachments_.find(&node);
  if (it == joinAttachments_.end()) {
    return std::nullopt;
  }
  return it->second.id;
}

std::shared_ptr<facebook::velox::exec::BaseHashTable> VeloxPrebuiltHashTables::getPrebuiltHashTable(
    const facebook::velox::core::HashJoinNode& node) {
  std::lock_guard<std::mutex> guard(mutex_);
  cleanupExpiredJoinAttachmentsLocked();
  auto it = joinAttachments_.find(&node);
  if (it == joinAttachments_.end()) {
    return nullptr;
  }
  return it->second.table;
}

void VeloxPrebuiltHashTables::detachPrebuiltHashTable(const facebook::velox::core::HashJoinNode& node) {
  std::lock_guard<std::mutex> guard(mutex_);
  cleanupExpiredJoinAttachmentsLocked();
  joinAttachments_.erase(&node);
}

} // namespace gluten
