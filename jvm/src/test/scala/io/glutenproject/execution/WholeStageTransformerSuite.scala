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

package io.glutenproject.execution

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.execution.ColumnarInputAdapter
import org.apache.spark.sql.test.SharedSparkSession

abstract class WholeStageTransformerSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  protected val backend: String
  protected val resourcePath: String
  protected val fileFormat: String

  protected var TPCHTables: Map[String, DataFrame] = _

  protected def createTPCHTables(): Unit = {
    TPCHTables = Seq(
      "customer",
      "lineitem",
      "nation",
      "orders",
      "part",
      "partsupp",
      "region",
      "supplier").map { table =>
      val tableDir = getClass.getResource(resourcePath).getFile
      val tablePath = new File(tableDir, table).getAbsolutePath
      val tableDF = spark.read.format(fileFormat).load(tablePath)
      tableDF.createOrReplaceTempView(table)
      (table, tableDF)
    }.toMap
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "io.glutenproject.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.gluten.sql.columnar.backend.lib", backend)
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.default.parallelism", "1")
      .set("spark.sql.shuffle.partitions", "1")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHTables()
  }

  test("generate hash join plan") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1"),
      ("spark.gluten.sql.columnar.forceshuffledhashjoin", "true")) {
      val df = spark.sql("""select l_partkey from
                           | lineitem join part join partsupp
                           | on l_partkey = p_partkey
                           | and l_suppkey = ps_suppkey""".stripMargin)
      val plan = df.queryExecution.executedPlan
      val joins = plan.collect {
        case shj: ShuffledHashJoinExecTransformer => shj
      }
      assert(joins.length == 2)

      // Children of Join should be seperated into different `TransformContext`s.
      assert(joins.forall(_.children.forall(_.isInstanceOf[ColumnarInputAdapter])))

      // WholeStageTransformerExec should be inserted for joins and its children separately.
      val wholeStages = plan.collect {
        case wst: WholeStageTransformerExec => wst
      }
      assert(wholeStages.length == 5)

      // Join should be in `TransformContext`
      val countSHJ = wholeStages.map {
        _.collectFirst {
          case _: ColumnarInputAdapter => 0
          case _: ShuffledHashJoinExecTransformer => 1
        }.getOrElse(0)
      }.sum
      assert(countSHJ == 2)
    }
  }
}
