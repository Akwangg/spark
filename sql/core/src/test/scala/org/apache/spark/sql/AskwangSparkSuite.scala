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

package org.apache.spark.sql

import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.util.ResetSystemProperties

@ExtendedSQLTest
class AskwangSparkSuite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper
  with ResetSystemProperties {

  setupTestData()

  test("create table/create table like/create table as select") {
    withTable("tb") {
      withSQLConf(SQLConf.PLAN_CHANGE_LOG_LEVEL.key -> "INFO",
        SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
        sql("CREATE TABLE `tb1`(i INT, dt TIMESTAMP) USING parquet")
        sql("show create table tb1").show(false)

        // sql("CREATE TABLE `tb2` like `tb1`")
      }
    }
  }

  test("sql query with filter timestamp") {
    withTable("tb") {
      withSQLConf(SQLConf.PLAN_CHANGE_LOG_LEVEL.key -> "INFO",
        SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
        sql("CREATE TABLE `tb`(i INT, dt TIMESTAMP) USING parquet")
        val ds = sql("INSERT INTO `tb` VALUES (1,cast(\"2024-04-11 11:01:00\" as Timestamp))")

        println("=================================================")
        val data = sql("SELECT * FROM `tb` where dt ='2024-04-11 11:01:00' ")

        println(data.queryExecution)
        println(data.show())
        println(data.explain(true))
      }
    }
  }

  /**
   * black.
   */
  test("writ pk table with pk null int type") {
    withTable("tb") {
      withSQLConf(SQLConf.PLAN_CHANGE_LOG_LEVEL.key -> "INFO",
        SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
        spark.sql(s"CREATE TABLE tb (id INT, dt string) " +
          s"using parquet " +
          s"TBLPROPERTIES ('primary-key'='id')")
        val ds = sql("INSERT INTO `tb` VALUES (cast(NULL as int),cast(NULL as string))")
        sql("SELECT * FROM `tb`").show()

        ds.explain(true)
      }
    }
  }
}