package com.haiteam

import org.apache.spark.sql.SparkSession

object Example_01 {
  def main(args: Array[String]): Unit = {

    val spark =SparkSession.builder().appName("hkproject").appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

  }

}
