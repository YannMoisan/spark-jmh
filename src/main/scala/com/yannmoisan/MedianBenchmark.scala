package com.yannmoisan

import org.openjdk.jmh.annotations.{Benchmark, Setup}
import scala.util.Random

class MedianBenchmark extends SparkBenchmark {
  val d = List.fill(10000) { Random.nextLong() }

  val median = new MedianComputation()

  @Setup override def setup(): Unit = {
    initSparkSession()
  }

  @Benchmark def udaf(): Unit = {
    val s2 = spark
    import s2.implicits._
    val res = spark
      .createDataset(d)
      .agg(median($"value"))
      .collect()
  }

  @Benchmark def builtin(): Unit = {
    val s2 = spark
    import s2.implicits._
    val res = spark
      .createDataset(d)
      .stat.approxQuantile("value", Array(0.5), 0)
  }

}
