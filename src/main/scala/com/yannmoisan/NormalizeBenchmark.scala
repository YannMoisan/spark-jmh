package com.yannmoisan

import org.openjdk.jmh.annotations.{Benchmark, Setup}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.util.Random

class NormalizeBenchmark extends SparkBenchmark {
  val d = List.fill(10000) { Seq(Random.nextDouble(), Random.nextDouble(), Random.nextDouble(), Random.nextDouble()) }

  val normalizeUdf = udf { (l: Seq[Double]) =>
    val max = l.max
    l.map(_ / max)
  }

  val optimizedNormalizeUdf = udf { (l: mutable.WrappedArray[Double]) =>
    val max = l.max
    (0 until 4).foreach(i => l.update(i, l(i)/max))
    l
  }

  @Setup override def setup(): Unit = {
    initSparkSession()
  }

  @Benchmark def udf_naive(): Unit = {
    val s2 = spark
    import s2.implicits._
    val res = spark
      .createDataset(d)
      .withColumn("values", normalizeUdf($"value"))
      .collect()
  }

  @Benchmark def udf_optimized(): Unit = {
    val s2 = spark
    import s2.implicits._
    val res = spark
      .createDataset(d)
      .withColumn("values", normalizeUdf($"value"))
      .collect()
  }

  @Benchmark def builtin(): Unit = {
    val s2 = spark
    import s2.implicits._
    val res = spark
      .createDataset(d)
      .withColumn("max", greatest((0 until 4).map(i => col("value")(i)): _*))
      .withColumn("values", array((0 until 4).map(i => col("value")(i) / col("max")): _*))
      .collect()
  }

}
