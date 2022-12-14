package com.banksy.common

import com.banksy.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {
  def start (master: String="local[*]", app: String="Application")(op : => Unit): Unit ={
    val sparkConf: SparkConf = new SparkConf().setMaster(master).setAppName(app)
    val sc = new SparkContext(sparkConf)
    EnvUtil.put(sc)

    try {
      op
    }catch {
      case ex => println(ex.getMessage)
    }

    // TODO 关闭连接
    sc.stop()
    EnvUtil.clear()
  }
}
