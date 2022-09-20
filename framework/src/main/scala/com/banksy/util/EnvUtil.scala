package com.banksy.util

import org.apache.spark.SparkContext

object EnvUtil {
  private val scLocal = new ThreadLocal[SparkContext]()
  //放
  def put(sc : SparkContext): Unit ={
    scLocal.set(sc)
  }
  //取
  def take(): SparkContext ={
    scLocal.get()
  }
  //删除
  def clear(): Unit ={
    scLocal.remove()
  }
}
