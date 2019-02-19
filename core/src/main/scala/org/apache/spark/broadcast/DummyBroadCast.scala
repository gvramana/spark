package org.apache.spark.broadcast

import scala.reflect.ClassTag

private [spark] class DummyBroadCast[T: ClassTag](var valueToBroadCast: T) extends Broadcast[T](0) {
  /**
    * Actually get the broadcasted value. Concrete implementations of Broadcast class must
    * define their own way to get the value.
    */
  override protected def getValue(): T = valueToBroadCast

  /**
    * Actually unpersist the broadcasted value on the executors. Concrete implementations of
    * Broadcast class must define their own logic to unpersist their own data.
    */
  override protected def doUnpersist(blocking: Boolean): Unit = {
    valueToBroadCast = null.asInstanceOf[T]
  }
  /**
    * Actually destroy all data and metadata related to this broadcast variable.
    * Implementation of Broadcast class must define their own logic to destroy their own
    * state.
    */
  override protected def doDestroy(blocking: Boolean): Unit = {
    valueToBroadCast = null.asInstanceOf[T]
  }
}
