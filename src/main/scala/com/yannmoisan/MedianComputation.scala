package com.yannmoisan

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, LongType, StructType}

class MedianComputation extends UserDefinedAggregateFunction {
  def inputSchema = new StructType()
    .add("value", LongType)

  def bufferSchema = new StructType()
    .add("values", ArrayType(LongType))

  def dataType = LongType

  def deterministic = true

  def initialize(buffer: MutableAggregationBuffer) =
    buffer.update(0, Seq.empty[Long])

  def evaluate(buffer: Row) = {
    val seconds = buffer.getSeq[Long](0)
    val size = seconds.length

    if (size != 0) {
      val median = (size / 2).floor.toInt
      seconds.sorted.apply(median)
    } else {
      0L
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    val seconds1 = buffer1.getSeq[Long](0)
    val seconds2 = buffer2.getSeq[Long](0)

    buffer1.update(0, seconds1 ++ seconds2)
  }

  def update(buffer: MutableAggregationBuffer, input: Row) = {
    val seconds = buffer.getSeq[Long](0)
    val cph = input.getLong(0)

    buffer.update(0, seconds :+ cph)
  }

}
