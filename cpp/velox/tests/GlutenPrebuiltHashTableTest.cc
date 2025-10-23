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
#include "substrait/SubstraitToVeloxPlan.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/MemoryManager.h"
#include "velox/exec/HashTableBuilder.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/utils/VectorMaker.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <google/protobuf/wrappers.pb.h>

#include <unordered_map>

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::core;

namespace gluten {
namespace {

const std::string kJoinFunctionName = "equal:i64_i64";
const uint32_t kJoinFunctionAnchor = 1;
const char* const kJoinTableId = "prebuilt-table";

void populateReadRel(::substrait::ReadRel* readRel, const std::string& name, const std::string& uri, bool nullable) {
  readRel->mutable_common()->mutable_direct();

  auto* baseSchema = readRel->mutable_base_schema();
  baseSchema->add_names(name);
  auto* structType = baseSchema->mutable_struct_();
  auto* childType = structType->add_types();
  auto* i64 = childType->mutable_i64();
  i64->set_nullability(
      nullable ? ::substrait::Type_Nullability_NULLABILITY_NULLABLE
               : ::substrait::Type_Nullability_NULLABILITY_REQUIRED);

  readRel->mutable_local_files()->add_items()->set_uri_file(uri);
}

::substrait::Plan makeJoinPlan(const std::string& tableId) {
  ::substrait::Plan plan;
  auto* extension = plan.add_extensions()->mutable_extension_function();
  extension->set_function_anchor(kJoinFunctionAnchor);
  extension->set_name(kJoinFunctionName);

  auto* relation = plan.add_relations()->mutable_root();
  auto* join = relation->mutable_input()->mutable_join();
  join->set_type(::substrait::JoinRel_JoinType_JOIN_TYPE_INNER);

  populateReadRel(join->mutable_left()->mutable_read(), "l_key", "iterator:0", false);
  populateReadRel(join->mutable_right()->mutable_read(), "r_key", "iterator:1", true);

  auto* scalar = join->mutable_expression()->mutable_scalar_function();
  scalar->set_function_reference(kJoinFunctionAnchor);
  scalar->mutable_output_type()->mutable_bool_()->set_nullability(::substrait::Type_Nullability_NULLABILITY_NULLABLE);

  auto* leftSelection = scalar->add_args()->mutable_value()->mutable_selection()->mutable_direct_reference();
  leftSelection->mutable_struct_field()->set_field(0);

  auto* rightSelection = scalar->add_args()->mutable_value()->mutable_selection()->mutable_direct_reference();
  rightSelection->mutable_struct_field()->set_field(1);

  google::protobuf::StringValue optimization;
  optimization.set_value("buildHashTableId=" + tableId + "\n");
  join->mutable_advanced_extension()->mutable_optimization()->PackFrom(optimization);

  return plan;
}

std::shared_ptr<BaseHashTable> buildHashTable(memory::MemoryPool* pool) {
  auto buildRowType = ROW({"r_key"}, {BIGINT()});
  HashTableBuilder builder(
      pool,
      buildRowType,
      std::vector<int32_t>{0},
      core::JoinType::kInner,
      false /*nullAware*/,
      false /*hasFilter*/,
      1 /*minTableSizeForParallelJoinBuild*/);

  facebook::velox::test::VectorMaker vectorMaker(pool);
  auto keys = vectorMaker.flatVector<int64_t>({1, 2, 3});
  auto rowVector =
      std::make_shared<RowVector>(pool, buildRowType, BufferPtr(nullptr), keys->size(), std::vector<VectorPtr>{keys});
  builder.addInput(rowVector);
  return builder.build();
}

void collectHashJoinNodes(const PlanNodePtr& node, std::vector<const HashJoinNode*>& joins) {
  if (!node) {
    return;
  }
  if (const auto* hashJoin = dynamic_cast<const HashJoinNode*>(node.get())) {
    joins.push_back(hashJoin);
  }
  for (const auto& source : node->sources()) {
    collectHashJoinNodes(source, joins);
  }
}

} // namespace

class GlutenPrebuiltHashTableTest : public ::testing::Test, public facebook::velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

TEST_F(GlutenPrebuiltHashTableTest, registerAndRetrieveSerializedTable) {
  auto table = buildHashTable(pool());
  const auto serialized = table->serialize();
  VeloxPrebuiltHashTables::registerSerialized(kJoinTableId, serialized, pool());

  auto stored = VeloxPrebuiltHashTables::get(kJoinTableId);
  ASSERT_NE(nullptr, stored);
  EXPECT_EQ(serialized, stored->serialize());

  VeloxPrebuiltHashTables::erase(kJoinTableId);
  EXPECT_EQ(nullptr, VeloxPrebuiltHashTables::get(kJoinTableId));
}

TEST_F(GlutenPrebuiltHashTableTest, convertsJoinWithPrebuiltHashTable) {
  auto table = buildHashTable(pool());
  VeloxPrebuiltHashTables::registerSerialized(kJoinTableId, table->serialize(), pool());
  auto registered = VeloxPrebuiltHashTables::get(kJoinTableId);
  ASSERT_NE(nullptr, registered);

  auto plan = makeJoinPlan(kJoinTableId);
  auto config = std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>{});
  SubstraitToVeloxPlanConverter converter(pool(), config.get(), std::nullopt, std::nullopt, true);
  auto veloxPlan = converter.toVeloxPlan(plan);

  std::vector<const HashJoinNode*> joins;
  collectHashJoinNodes(veloxPlan, joins);
  ASSERT_EQ(joins.size(), 1);
  const auto* join = joins.front();
  EXPECT_TRUE(VeloxPrebuiltHashTables::hasPrebuiltHashTable(*join));
  auto attachedId = VeloxPrebuiltHashTables::getPrebuiltHashTableId(*join);
  ASSERT_TRUE(attachedId.has_value());
  EXPECT_EQ(kJoinTableId, attachedId.value());
  EXPECT_EQ(registered.get(), VeloxPrebuiltHashTables::getPrebuiltHashTable(*join).get());

  VeloxPrebuiltHashTables::erase(kJoinTableId);
}

TEST_F(GlutenPrebuiltHashTableTest, missingPrebuiltHashTableFailsConversion) {
  VeloxPrebuiltHashTables::erase(kJoinTableId);
  auto plan = makeJoinPlan(kJoinTableId);
  auto config = std::make_shared<facebook::velox::config::ConfigBase>(std::unordered_map<std::string, std::string>{});
  SubstraitToVeloxPlanConverter converter(pool(), config.get(), std::nullopt, std::nullopt, true);

  EXPECT_THROW(converter.toVeloxPlan(plan), VeloxException);
}

} // namespace gluten
