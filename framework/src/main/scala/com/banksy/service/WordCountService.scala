package com.banksy.service

import com.banksy.common.TService
import com.banksy.dao.WordCountDao

/**
 * 服务层
 */
class WordCountService extends TService{
  val wordCountDao = new WordCountDao
  def dataAnalysis() ={
    val RDDlines = wordCountDao.readFile("components/datas/1.txt")
    val RDDwords = RDDlines.flatMap(_.split(" "))
    val RDDwordToOne = RDDwords.map(word => (word, 1))
    val RDDwordToSum = RDDwordToOne.reduceByKey(_+_)
    val array = RDDwordToSum.collect()
    array
  }
}
