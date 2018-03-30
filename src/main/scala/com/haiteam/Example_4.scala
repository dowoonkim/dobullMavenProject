package com.haiteam

import org.apache.spark.sql.SparkSession

object Example_4 {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().appName("hkproject").
      config("spark.master", "local").
      getOrCreate()

    var staticUrl = "jdbc:oracle:thin:@192.168.110.19:1522/XE"
    var staticUser = "DWKIM"
    var staticPw = "dwkim"
    var selloutDb = "KOPO_PRODUCT_VOLUME2"

    val selloutDataFromOracle= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromOracle.createOrReplaceTempView("selloutTable")
    selloutDataFromOracle.show()
  }
}
