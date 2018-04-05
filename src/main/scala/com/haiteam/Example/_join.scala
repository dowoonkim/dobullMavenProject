package com.haiteam.Example

import org.apache.spark.sql.SparkSession;


object _join {
  val spark =SparkSession.builder().appName("hkproject").
    config("spark.master", "local").
    getOrCreate()


  var mainFile = "kopo_channel_seasonality_ex.csv"
  var subFile = "kopo_product_mst.csv"
  var dataPath = "c:/spark/bin/data/"

  var mainData = spark.read.format("csv").
    option("header", "true").load(dataPath + mainFile)
  var subData = spark.read.format("csv").
    option("header", "true").load(dataPath + subFile)

  mainData.createTempView("maindata")
  subData.createTempView("subdata")

  var leftJoinData = spark.sql("select a.*, b.productname " +
    "from maindata a left outer join subdata b " +
    "on a.productgroup = b.productid")

}

