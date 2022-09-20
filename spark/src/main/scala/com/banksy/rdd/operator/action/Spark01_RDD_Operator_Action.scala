package com.banksy.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // TODO 行动算子
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    val rddReduce = rdd.reduce(_ + _)

    println("[reduce]：" + rddReduce)

    // TODO 关闭环境
    sc.stop()
  }
}
