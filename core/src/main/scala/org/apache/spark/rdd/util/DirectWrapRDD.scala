package org.apache.spark.rdd.util

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MeasureTime
import org.apache.spark.{Partition, TaskContext}

/**
  * RDD lineage without shuffle wrapping current rdd lineage with DirectWrapRDD will help
  * spark scheduler to schedule better.
  */
private[spark] class DirectWrapRDD[T: ClassTag](
  @transient var prevRdd: RDD[T],
  override val checkpointEnabled: Boolean = false)
  extends DirectRDD[T](prevRdd.sparkContext, prevRdd, checkpointEnabled) {

  @transient override val partitioner = firstParent[T].partitioner

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[T] =
    firstParent[T].iterator(split, context)

}