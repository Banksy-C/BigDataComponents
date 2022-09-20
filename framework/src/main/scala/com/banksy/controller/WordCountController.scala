package com.banksy.controller

import com.banksy.common.TController
import com.banksy.service.WordCountService

/**
 * 控制层
 */
class WordCountController extends TController{
  val wordCountService = new WordCountService

  def dispatch(): Unit ={
    val array = wordCountService.dataAnalysis()
    array.foreach(println)
  }
}
