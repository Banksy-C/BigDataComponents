package com.banksy.rdd.framework.application

import com.banksy.rdd.framework.controller.WordCountController
import org.apache.spark.{SparkConf, SparkContext}

object WordCountApplication extends App {

  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
  val sc = new SparkContext(sparkConf)

  val wordCountController = new WordCountController
  wordCountController.dispatch()

  sc.stop()
}
