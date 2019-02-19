package org.apache.spark.rdd.util

import scala.reflect.ClassTag

import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MeasureTime
import org.apache.spark.util.CallSite

/**
  * RDD lineage without parent rdd, will improve serialization and deserialization of rdd
  * during scheduling.
  */
abstract private[spark] class DirectRDD[T: ClassTag](
  @transient private var _sc: SparkContext,
  @transient var prev: RDD[T],
  val checkpointEnabled: Boolean = false)
  extends RDD[T](_sc,
    if(prev != null) List(new OneToOneDependency(prev)) else Nil) {

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }

  override def doCheckpoint(): Unit ={
    if(checkpointEnabled){
      super.doCheckpoint()
    }
  }

  override def collect(): Array[T] =  {
    collect(false)
  }

  def collect(cleanFunc: Boolean): Array[T] =  {
    MeasureTime.end("DAG scheduler submit only-0", "initial")
    MeasureTime.start("DAG scheduler submit only-1")
    val partitionsLen = 0 until partitions.length
    MeasureTime.end("DAG scheduler submit only-1", "just get partitions")
    MeasureTime.start("DAG scheduler submit only-2")
    val func = (iter: Iterator[T]) => iter.toArray
    val cleanedFunc = if(cleanFunc) sparkContext.clean(func) else func
    val newFunc = (ctx: TaskContext, it: Iterator[T]) => cleanedFunc(it)
    MeasureTime.end("DAG scheduler submit only-2", "clean closure function")

    MeasureTime.start("DAG scheduler submit only-3")

    val results = new Array[Array[T]](partitionsLen.size)
    runJob(newFunc, partitionsLen, (index, res: Array[T]) => results(index) = res)

    Array.concat(results: _*)
  }

  def getFunc(cleanFunc: Boolean) = {
    val func = (iter: Iterator[T]) => iter.toArray
    val cleanedFunc = if(cleanFunc) sparkContext.clean(func) else func
    (ctx: TaskContext, it: Iterator[T]) => cleanedFunc(it)
  }

  /**
    * Code copied from spark Context runJob and removed repeated clean function call
    * as clean is taking minimum 2-3 milli sec to execute
    */
  def runJob[U: ClassTag](
    cleanedFunc: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    resultHandler: (Int, U) => Unit): Unit = {
    if (sparkContext.stopped.get()) {
      throw new IllegalStateException("SparkContext has been shutdown")
    }
    val callSite = CallSite("","")
    logInfo("Starting job: " + callSite.shortForm)
    if (sparkContext.conf.getBoolean("spark.logLineage", false)) {
      logInfo("RDD's recursive dependencies:\n" + this.toDebugString)
    }
    sparkContext.dagScheduler.runJob(this, cleanedFunc, partitions,
      callSite, resultHandler, sparkContext.localProperties.get)
    sparkContext.progressBar.foreach(_.finishAll())
    doCheckpoint()
  }

  private def getCallSite(): CallSite ={
    //this function is taking 2ms
    import sun.misc.SharedSecrets
    val access = SharedSecrets.getJavaLangAccess
    val exception = new Exception()
    val depth = access.getStackTraceDepth(exception)
    lazy val shortForm =
      (2 to 5).filter(_ < depth).map(access.getStackTraceElement(exception,_)).map(
      element => element.getClassName+":"+element.getMethodName+":"+element.getLineNumber).fold("")(_+"\n"+_)
    CallSite(
      Option(_sc.getLocalProperty(CallSite.SHORT_FORM)).getOrElse(""),
      Option(_sc.getLocalProperty(CallSite.LONG_FORM)).getOrElse("")
    )
  }
}