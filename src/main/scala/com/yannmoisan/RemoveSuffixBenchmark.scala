package com.yannmoisan

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}
import org.openjdk.jmh.annotations._

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
abstract class SparkBenchmark {
  var spark: SparkSession = _

  def setup(): Unit

  val removeSuffixUdf = udf { (id: String) =>
    if (id != null && id.endsWith("a")) {
      id.dropRight(1)
    } else {
      id
    }
  }

  def removeSuffix(c: Column) = when(c.endsWith("a"), c.substr(lit(0), length(c) - 1)).otherwise(c)

  val data = List.fill(10000) {UUID.randomUUID().toString}

  def initSparkSession(): Unit =
    spark = SparkSession.builder()
      .master("local")
      .appName("benchmark")
      .getOrCreate()

  @TearDown def tearDown(): Unit = if (spark != null) spark.stop()
}

class RemoveSuffixBenchmark extends SparkBenchmark {

  @Setup override def setup(): Unit = {
    initSparkSession()
  }

  @Benchmark def udf(): Unit = {
    val s2 = spark
    import s2.implicits._
    val res = spark
      .createDataset(data)
      .withColumn("col", removeSuffixUdf($"value"))
      .collect()
  }

  @Benchmark def builtin_regexp_replace(): Unit = {
    val s2 = spark
    import s2.implicits._
    val res = spark
      .createDataset(data)
      .withColumn("col", regexp_replace($"value", "a$", ""))
      .collect()
  }

  @Benchmark def builtin_optimized(): Unit = {
    val s2 = spark
    import s2.implicits._
    val res = spark
      .createDataset(data)
      .withColumn("col", removeSuffix($"value"))
      .collect()
  }

}
