package com.banksy.common

import com.banksy.util.EnvUtil

trait TDao {
  def readFile(path:String) ={
    EnvUtil.take().textFile(path)
  }
}
