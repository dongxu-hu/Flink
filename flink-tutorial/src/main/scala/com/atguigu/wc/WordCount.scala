package com.atguigu.wc

import org.apache.flink.api.scala._


// 批处理
object WordCount {
  def main(args: Array[String]): Unit = {

    //1. 创建一个批处理的执行环境
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //2. 从文件中读取数据
    var inputpath= "F:\\MyWork\\GitDatabase\\flink-programing\\flink-tutorial\\src\\main\\resources\\hello.txt"
    val inpuDataSet: DataSet[String] = environment.readTextFile(inputpath)

    //3. 空格分词，map成计数的二元组
    val resultDataSet: AggregateDataSet[(String, Int)] = inpuDataSet.flatMap(_.split(" ")).map((_, 1))
      .groupBy(0) //以第一个元素，即以word作为分组
      .sum(1)   //对元组中第二个元素求和

    resultDataSet.print()



  }
}
