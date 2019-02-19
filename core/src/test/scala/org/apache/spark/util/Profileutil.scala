package org.apache.spark.util

/**
  * profiling code.
  */
object Profileutil {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println( "")
    println( "Elapsed time: " + (t1 - t0) + "ns, " + (t1 - t0)/1000000 + "ms")
    result
  }

  def timec[R](comment: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println( "")
    println( "^^^^^^^^^^^^^^^^^^^^^^^^" + comment +
      "-> Elapsed time: " + (t1 - t0) + "ns, " + (t1 - t0)/1000000 + "ms")
    println( "")
    result
  }
}
