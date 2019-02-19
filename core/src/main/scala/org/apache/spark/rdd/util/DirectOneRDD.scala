package org.apache.spark.rdd.util

import scala.reflect.ClassTag

import org.apache.spark.SparkContext

/**
  * RDD lineage without shuffle
  */
abstract private[spark] class DirectOneRDD[T: ClassTag](
  @transient private var _sc: SparkContext,
  override val checkpointEnabled: Boolean = false)
  extends DirectRDD[T](_sc, null, checkpointEnabled) {

  override private[spark] def withScope[U](body: => U): U = body

}