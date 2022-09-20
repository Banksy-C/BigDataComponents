package com.banksy.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


object Spark02_Streaming_Queue {
  def main(args: Array[String]): Unit = {
    //  TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //  TODO 逻辑处理
    //  创建RDD队列
    val rddQueue = new mutable.Queue[RDD[Int]]()
    //  创建 QueueInputDStream
    val inputStream = ssc.queueStream(rddQueue, oneAtATime = false)
    //  处理队列中的 RDD 数据
    val mappedStream = inputStream.map((_, 1))
    val reducedStream = mappedStream.reduceByKey(_+_)
    //打印结果
    reducedStream.print()
    //  TODO 启动采集器
    ssc.start()
    //循环创建并向 RDD 队列中放入 RDD
    for (i <- 1 to 5) {
    rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
    Thread.sleep(2000)
    }
    ssc.awaitTermination()
  }

}
