package com.banksy.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Spark01_Streaming_WordCount {
  def main(args: Array[String]): Unit = {
    //  TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //  TODO 逻辑处理
    //  获取端口处理
    val RDDlines = ssc.socketTextStream("localhost", 9999)
    //  逻辑
    val RDDwords = RDDlines.flatMap(_.split(" "))
    val RDDwordTone = RDDwords.map((_, 1))
    val RDDwordCount = RDDwordTone.reduceByKey(_+_)
    RDDwordCount.print()

    //  TODO 启动采集器
    ssc.start()
    ssc.awaitTermination()
  }
}
