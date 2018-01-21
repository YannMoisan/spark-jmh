package com.yannmoisan

import org.openjdk.jmh.annotations.{Benchmark, Setup}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.util.Random

class CombineMapBenchmark extends SparkBenchmark {
  val d = List.fill(10000) {
    ("ignore", Map(
      "A" -> Random.nextInt(10),
      "B" -> Random.nextInt(10),
      "C" -> Random.nextInt(10),
      "D" -> Random.nextInt(10)
    ))
  }

  val combineMaps = new CombineMaps[String, Int](StringType, IntegerType, _ + _)

  @Setup override def setup(): Unit = {
    initSparkSession()
  }

  @Benchmark def udaf(): Unit = {
    val s2 = spark
    import s2.implicits._
    val res = spark
      .createDataset(d)
      .agg(combineMaps($"_2"))
      .collect()
  }

  @Benchmark def builtin(): Unit = {
    val s2 = spark
    import s2.implicits._
    val res = spark
      .createDataset(d)
      .agg(
        sum($"_2.A"),
        sum($"_2.B"),
        sum($"_2.C"),
        sum($"_2.D")
      )
      .collect()
  }

}
