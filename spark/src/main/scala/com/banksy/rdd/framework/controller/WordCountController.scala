package com.banksy.rdd.framework.controller

import com.banksy.rdd.framework.service.WordCountService

/**
 * 控制层
 */
class WordCountController {
  val wordCountService = new WordCountService
  def dispatch(): Unit ={
    val array = wordCountService.dataAnalysis()
    array.foreach(println)
  }
}
