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
#include <optional>

#include "velox/core/PlanNode.h"

#include "velox/common/memory/MemoryPool.h"
#include "velox/exec/HashTable.h"

namespace gluten {

class VeloxPrebuiltHashTables {
 public:
  static void registerSerialized(
      const std::string& id,
      const std::string& serialized,
      facebook::velox::memory::MemoryPool* pool);

  static std::shared_ptr<facebook::velox::exec::BaseHashTable> get(const std::string& id);

  static void erase(const std::string& id);

  static void attachPrebuiltHashTable(
      const std::shared_ptr<const facebook::velox::core::HashJoinNode>& node,
      std::string id,
      std::shared_ptr<facebook::velox::exec::BaseHashTable> table);

  static bool hasPrebuiltHashTable(const facebook::velox::core::HashJoinNode& node);

  static std::optional<std::string> getPrebuiltHashTableId(const facebook::velox::core::HashJoinNode& node);

  static std::shared_ptr<facebook::velox::exec::BaseHashTable> getPrebuiltHashTable(
      const facebook::velox::core::HashJoinNode& node);

  static void detachPrebuiltHashTable(const facebook::velox::core::HashJoinNode& node);

 private:
  static std::unordered_map<std::string, std::shared_ptr<facebook::velox::exec::BaseHashTable>> tables_;
  struct HashJoinAttachment {
    std::weak_ptr<const facebook::velox::core::HashJoinNode> node;
    std::string id;
    std::shared_ptr<facebook::velox::exec::BaseHashTable> table;
  };
  static std::unordered_map<const facebook::velox::core::HashJoinNode*, HashJoinAttachment> joinAttachments_;

  static void cleanupExpiredJoinAttachmentsLocked();
  static std::mutex mutex_;
};

} // namespace gluten
