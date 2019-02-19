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
package org.apache.spark.examples.sql.hive

// $example on:spark_hive$
import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.RuleExecutor
// $example off:spark_hive$

object SparkSchedulerRamana {

  // $example on:spark_hive$
  case class Record(key: Int, value: String)
  // $example off:spark_hive$

  def main(args: Array[String]) {
    // When working with Hive, one must instantiate `SparkSession` with Hive support, including
    // connectivity to a persistent Hive metastore, support for Hive serdes, and Hive user-defined
    // functions. Users who do not have an existing Hive deployment can still enable Hive support.
    // When not configured by the hive-site.xml, the context automatically creates `metastore_db`
    // in the current directory and creates a directory configured by `spark.sql.warehouse.dir`,
    // which defaults to the directory `spark-warehouse` in the current directory that the spark
    // application is started.

    // $example on:spark_hive$
    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM src").show()
    // +---+-------+
    // |key|  value|
    // +---+-------+
    // |238|val_238|
    // | 86| val_86|
    // |311|val_311|
    // ...

    // Aggregation queries are also supported.
    sql("SELECT COUNT(*) FROM src").show()
    // +--------+
    // |count(1)|
    // +--------+
    // |    500 |
    // +--------+

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    var sqlDF = profileutil.timec("compile: select 1",
      sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key"))

    RuleExecutor.resetMetrics()
    sqlDF = profileutil.timec("compile: select 2",
        sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key"))
    profileutil.timec("optimize : select 1", sqlDF.queryExecution.optimizedPlan)
    profileutil.timec("physical : select 1", sqlDF.queryExecution.sparkPlan)
    profileutil.timec("optimize : select 2", sqlDF.queryExecution.optimizedPlan)
    profileutil.timec("physical : select 2", sqlDF.queryExecution.sparkPlan)
    println("^^^^^^^^^^^^^^^^" + RuleExecutor.dumpTimeSpent())
    // The items in DataFrames are of type Row, which allows you to access each column by ordinal.
    profileutil.timec("execute: select 1", sqlDF.count())
    profileutil.timec("execute: select 2", sqlDF.count())
    profileutil.timec("execute: select 3", sqlDF.count())

    spark.stop()
    // $example off:spark_hive$
  }
}
