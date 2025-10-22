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

#include <gtest/gtest.h>

#include <algorithm>
#include <limits>
#include <optional>
#include <vector>

#include "velox/exec/HashTable.h"
#include "velox/exec/RowContainer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace gluten {

using facebook::velox::BaseVector;
using facebook::velox::asRowType;
using facebook::velox::RowVector;
using facebook::velox::RowVectorPtr;
using facebook::velox::memory::MemoryManager;
using facebook::velox::exec::BaseHashTable;
using facebook::velox::exec::HashTable;
using facebook::velox::exec::RowContainer;
using facebook::velox::exec::RowContainerIterator;
using facebook::velox::test::assertEqualVectors;

class VeloxHashTableSerializationTest
    : public ::testing::Test,
      public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    MemoryManager::testingSetInstance(MemoryManager::Options{});
  }

  BaseHashTable::HashTableBuildInfo makeInfo(
      const RowVectorPtr& sample,
      uint32_t numKeys,
      bool ignoreNullKeys) {
    BaseHashTable::HashTableBuildInfo info;
    info.tableType = asRowType(sample->type());
    info.numKeys = numKeys;
    info.ignoreNullKeys = ignoreNullKeys;
    info.allowDuplicates = true;
    info.isJoinBuild = true;
    info.hasProbedFlag = false;
    info.minTableSizeForParallelJoinBuild = 0;
    return info;
  }

  RowVectorPtr extractRows(
      const BaseHashTable::HashTableBuildInfo& info,
      BaseHashTable* table) {
    auto container = table->rows();
    const auto totalRows = container->numRows();
    auto result = std::dynamic_pointer_cast<RowVector>(
        BaseVector::create(info.tableType, totalRows, pool()));
    if (totalRows == 0) {
      return result;
    }

    constexpr int32_t kBatchSize = 1024;
    std::vector<char*> rowPointers(totalRows);
    RowContainerIterator iter;
    int32_t offset = 0;
    while (offset < totalRows) {
      const auto remaining = totalRows - offset;
      const auto batch = std::min<int32_t>(kBatchSize, remaining);
      const auto received = container->listRows(
          &iter,
          batch,
          RowContainer::kUnlimited,
          rowPointers.data() + offset);
      EXPECT_GT(received, 0);
      if (received == 0) {
        break;
      }
      offset += received;
    }
    EXPECT_EQ(offset, totalRows);

    for (int32_t i = 0; i < info.tableType->size(); ++i) {
      result->childAt(i)->resize(totalRows);
      RowContainer::extractColumn(
          reinterpret_cast<const char* const*>(rowPointers.data()),
          totalRows,
          container->columnAt(i),
          container->columnHasNulls(i),
          0,
          result->childAt(i));
    }
    return result;
  }
};

TEST_F(VeloxHashTableSerializationTest, createFromRowVectorsRoundTrip) {
  auto batch1 = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeFlatVector<int32_t>({10, 20, 30})});
  auto batch2 = makeRowVector({
      makeFlatVector<int64_t>({4, 5}),
      makeFlatVector<int32_t>({40, 50})});

  auto info = makeInfo(batch1, /*numKeys=*/1, /*ignoreNullKeys=*/false);
  std::vector<RowVectorPtr> input{batch1, batch2};

  auto table = HashTable<false>::createFromRowVectors(info, input, pool());
  ASSERT_EQ(table->rows()->numRows(), 5);

  auto expected = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      makeFlatVector<int32_t>({10, 20, 30, 40, 50})});
  assertEqualVectors(expected, extractRows(info, table.get()));

  auto serialized = table->serialize();
  EXPECT_EQ(serialized.info.numKeys, info.numKeys);
  EXPECT_FALSE(serialized.serializedRows.empty());

  auto roundTripped = BaseHashTable::deserialize(serialized, pool());
  assertEqualVectors(expected, extractRows(info, roundTripped.get()));
}

TEST_F(VeloxHashTableSerializationTest, ignoresNullKeysDuringBuild) {
  auto batch = makeRowVector({
      makeNullableFlatVector<int64_t>({1, std::nullopt, 3, std::nullopt}),
      makeFlatVector<int32_t>({10, 20, 30, 40})});

  auto info = makeInfo(batch, /*numKeys=*/1, /*ignoreNullKeys=*/true);
  std::vector<RowVectorPtr> input{batch};

  auto table = HashTable<true>::createFromRowVectors(info, input, pool());
  ASSERT_EQ(table->rows()->numRows(), 2);

  auto expected = makeRowVector({
      makeNullableFlatVector<int64_t>({1, 3}),
      makeFlatVector<int32_t>({10, 30})});
  assertEqualVectors(expected, extractRows(info, table.get()));

  auto serialized = table->serialize();
  EXPECT_TRUE(serialized.info.ignoreNullKeys);
  auto roundTripped = BaseHashTable::deserialize(serialized, pool());
  assertEqualVectors(expected, extractRows(info, roundTripped.get()));
}

} // namespace gluten
