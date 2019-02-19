package org.apache.spark.scheduler

import java.util.concurrent.ConcurrentHashMap

object MeasureTime {
  val strToTime: ConcurrentHashMap[String,(Long, Long, String)]
    = new ConcurrentHashMap[String,(Long, Long, String)]()
  def start(id: String): Unit ={
    strToTime.put(id, (System.nanoTime(),0, ""));
  }
  def end(id: String, comment: String =""): Unit ={
    val value = strToTime.get(id)
    if(value != null) {
      strToTime.put(id, (value._1, System.nanoTime(), comment));
    }
  }

  def getDurationNano(id: String): Long = {
    val value = strToTime.get(id)
    if(value != null) {
      value._2 - value._1
    } else {
      0
    }
  }

  def getDurationMilli(id: String): Long = {
    getDurationNano(id)/1000000;
  }

  def printDurationMilli(id: String) = {
    val value = strToTime.get(id)
    val comment = if(value != null && value._3.length > 0) "("+value._3+")" else ""
    println(">>>>> Duration " + id + comment + " (ms): " + getDurationNano(id)/1000000.0)
  }

  def printDurationNano(id: String) =  {
    val value = strToTime.get(id)
    val comment = if(value != null && value._3.length > 0) "("+value._3+")" else ""
    println(">>>>> Duration " + id + comment + " (nano): " + getDurationNano(id))
  }

  def reset(): Unit ={
    strToTime.clear();
  }
  def printAllMilli()={
    import scala.collection.JavaConverters._
    strToTime.asScala.toList.sortBy(entry => entry._2._1).foreach( entry => printDurationMilli(entry._1))
  }

  def printAllNano()={
    import scala.collection.JavaConverters._
    strToTime.asScala.toList.sortBy(entry => entry._2._1).foreach( entry => printDurationNano(entry._1))
  }
}
