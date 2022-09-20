package com.banksy.application

import com.banksy.common.{TApplication, TController}
import com.banksy.controller.WordCountController
import org.apache.spark.{SparkConf, SparkContext}

object WordCountApplication extends App with TApplication{

  start("local[*]","WordCount"){
    val wordCountController = new WordCountController
    wordCountController.dispatch()
  }
}
