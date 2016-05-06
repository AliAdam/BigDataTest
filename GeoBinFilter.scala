package com.esri

import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  */
object GeoCount extends App with Logging {
  val sc = new SparkContext(new SparkConf())
  try {
    val cell = 0.001
    val half = cell * 0.5
    sc.textFile("hdfs:///user/root/gps2").
      map(line => {
        try {
          val splits = line.split(',')
          val c = (splits(1).toDouble / cell).toInt
          val r = (splits(2).toDouble / cell).toInt
          val w = (splits(4)).toInt
          val h = (splits(5)).toInt
          (r, c, w, h) -> 1
        } catch {
          case _: Throwable => (0, 0, 0, 0) -> 0
        }
      }).
      filter {
        case ((r, c, w, h), count) => r != 0 && c != 0
      }.
      reduceByKey((a, b) => a + b).
      filter {
        case ((r, c, w, h), count) => count > 2
      }.
      map {
        case ((r, c, w, h), count) => {
          val x = c * cell + half
          val y = r * cell + half
          f"$x%.6f,$y%.6f,$w,$h,$count"
        }
      }.
      saveAsTextFile("hdfs:///tmp/bin2")
  } finally {
    sc.stop()
  }
}
