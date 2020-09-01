package com.atguigu.window

import com.atguigu.api.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val DataStream: DataStream[String] = environment.socketTextStream("hadoop202", 7777)
    val daStream: DataStream[SensorReading] = DataStream.map(date => {
      val split: Array[String] = date.split(",")
      SensorReading(split(0).trim, split(1).trim.toLong, split(2).trim.toDouble)
    })
    val result: DataStream[SensorReading] = daStream.keyBy(_.id).timeWindow(Time.seconds(10))minBy(2)
    result.print()

    environment.execute()

  }

}
