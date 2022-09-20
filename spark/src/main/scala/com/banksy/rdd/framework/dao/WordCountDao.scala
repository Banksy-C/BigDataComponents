package com.banksy.rdd.framework.dao

import com.banksy.rdd.framework.application.WordCountApplication.sc

/**
 * 持久层
 */
class WordCountDao {
  def readFile(path:String) ={
    sc.textFile(path)
  }
}
