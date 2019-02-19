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

package org.apache.spark.scheduler

import org.apache.spark._
import org.apache.spark.rdd.util.{DirectRDD, DirectWrapRDD}
import org.apache.spark.util.Profileutil

object SpeedSchedulingSuiteState {
  var tasksRun = 0

  def clear(): Unit = {
    tasksRun = 0
  }
}

/*
  Intial test value before optimization
  collect time -> Elapsed time: 51399290ns, 51ms
  Dagscheduler time -> 36ms
 */
class SpeedSchedulingSuite extends SparkFunSuite with LocalSparkContext {
  test("Speed schedule DirectWrapRDD test") {
    try {
      val conf = new SparkConf
      conf.set("spark.default.parallelism", "1")
      // without kryo serialization and broadcast is taking 6 sec.
      // with kryo serialization and broadcast is taking 21 sec.
      // conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      sc = new SparkContext("local[1]", "test", conf)
      // sc = new SparkContext("local-cluster[1, 1, 1024]", "test", conf)
      // sc = new SparkContext("spark://localhost:7077", "test", conf)
      val rdd = sc.parallelize(1 to 3, 3).map { x =>
        SpeedSchedulingSuiteState.tasksRun += 1
        (x, x)
      }
      val rdd1  = rdd.filter( x => x._1 == 1)
      val warmupRDD = new DirectWrapRDD(rdd1);
      // warm up
      val result1 = warmupRDD.collect()
      assert(result1.toSet == Set((1, 1)))
      // warm up end

      MeasureTime.reset()

      val directRDD = new DirectWrapRDD(rdd1);
      MeasureTime.start("DAG scheduler submit only")
      MeasureTime.start("DAG scheduler submit only-0")
      val result = directRDD.collect()

      MeasureTime.printAllMilli()
      // MeasureTime.printAllNano()

      assert(SpeedSchedulingSuiteState.tasksRun == 6)
      assert(result.toSet == Set((1, 1)))
      assert(SpeedSchedulingSuiteState.tasksRun == 6)
    } finally {
      SpeedSchedulingSuiteState.clear()
    }
  }

  test("Speed schedule DirectOneRDD test") {
    try {
      val conf = new SparkConf
      conf.set("spark.default.parallelism", "1")
      // without kryo serialization and broadcast is taking 6 sec.
      // with kryo serialization and broadcast is taking 21 sec.
      // conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      sc = new SparkContext("local[1]", "test", conf)
      // sc = new SparkContext("local-cluster[1, 1, 1024]", "test", conf)
      // sc = new SparkContext("spark://localhost:7077", "test", conf)
      val rdd = sc.parallelize(1 to 3, 3).map { x =>
        SpeedSchedulingSuiteState.tasksRun += 1
        (x, x)
      }
      val rdd1  = rdd.filter( x => x._1 == 1)
      // warm up
      val result1 = rdd1.collect()
      assert(result1.toSet == Set((1, 1)))
      // warm up end

      val directRDD = new CustomDirectOneRDD(sc);
      MeasureTime.start("DAG scheduler submit only")
      MeasureTime.start("DAG scheduler submit only-0")
      val result = directRDD.collect()

      MeasureTime.printAllMilli()
      // MeasureTime.printAllNano()

      assert(SpeedSchedulingSuiteState.tasksRun == 3)
      assert(result.toSet == Set(1, 2, 3))
      assert(SpeedSchedulingSuiteState.tasksRun == 3)
    } finally {
      SpeedSchedulingSuiteState.clear()
    }
  }
}
