package com.atguigu

import com.atguigu.api.SensorReading
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessTest2 {

  def main(args: Array[String]): Unit = {
    //  环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //容错机制相关配置
    env.enableCheckpointing(1000l) // 默认500 毫秒, 生产检查点时间间隔
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)  // 精准1次性
    env.getCheckpointConfig.setCheckpointInterval(60000L)   //
    env.getCheckpointConfig.setCheckpointTimeout(2)  //最大checkpoint个数
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)  //最大checkpoint个数
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L) // 上一个检查点结束到下一个检查点开启的时间间隔；此时默认checkpoint并行数为1；
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(2) // 运行checkpoint失败次数，默认0，表示检查点失败代表任务也失败

    //重启策略：   重启尝试2次，每次时间间隔500ms；
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2,100L))




    val inputStream: DataStream[String] = env.socketTextStream("hadoop202", 7777)

    val dateStream: DataStream[SensorReading] = inputStream.map(date => {
      val split: Array[String] = date.split(",")
      SensorReading(split(0).trim, split(1).trim.toLong, split(2).trim.toDouble)
    })

    // 使用测输出流实现分流操作，定义主流为高温流
    val highStream = dateStream.process( new SpiltStreamOp(30.0))

    highStream.print("high")

    val lowStream = highStream.getSideOutput(new OutputTag[(String, Double,Long)]("low"))
    lowStream.print("low")


    env.execute("process function test")

  }
}

// 自定义keyed process function 实现10秒内温度连续上升报警检测
class SpiltStreamOp(time: Double) extends ProcessFunction[SensorReading,SensorReading]{
  override def processElement(i: SensorReading,
                              context: ProcessFunction[SensorReading,
                                SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    if(i.temperature > time){
      collector.collect(i)
    } else {
      // 如果小于等于，输出到低温流
      context.output(new OutputTag[(String,Double,Long)]("low"),(i.id,i.temperature,i.timestamp))
    }

  }
}



