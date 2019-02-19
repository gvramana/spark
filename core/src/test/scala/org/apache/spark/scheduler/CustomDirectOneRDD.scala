package org.apache.spark.scheduler;

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.util.DirectOneRDD

/**
 * Custom implementation
 */
private[spark] class CustomDirectOneRDD(
    @transient private var _sc: SparkContext,
    override val checkpointEnabled: Boolean = false)
    extends DirectOneRDD[Int](_sc, checkpointEnabled) {
    /**
      * :: DeveloperApi ::
      * Implemented by subclasses to compute a given partition.
      */
    override def compute(split: Partition,
        context: TaskContext): Iterator[Int] = {
        new Iterator[Int] {
            var currentRef: Int = 1;
            override def hasNext: Boolean = (currentRef <= 3)

            override def next(): Int = {
                currentRef = currentRef + 1
                currentRef-1;
            }
        }
    }

    /**
      * Implemented by subclasses to return the set of partitions in this RDD. This method will only
      * be called once, so it is safe to implement a time-consuming computation in it.
      *
      * The partitions in this array must satisfy the following property:
      * `rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
      */
    override protected def getPartitions: Array[Partition] =
        (0 to 2).map(i => new TestPartition(i, i)).toArray
}

class TestPartition(i: Int, value: Int) extends Partition with Serializable {
    def index: Int = i
    def testValue: Int = this.value
}
