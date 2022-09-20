package com.banksy.rdd.framework.service

import com.banksy.rdd.framework.dao.WordCountDao

/**
 * 服务层
 */
class WordCountService {
  val wordCountDao = new WordCountDao
  def dataAnalysis() ={
    val RDDlines = wordCountDao.readFile("spark/datas/1.txt")
    val RDDwords = RDDlines.flatMap(_.split(" "))
    val RDDwordToOne = RDDwords.map(word => (word, 1))
    val RDDwordToSum = RDDwordToOne.reduceByKey(_+_)
    val array = RDDwordToSum.collect()
    array
  }
}
