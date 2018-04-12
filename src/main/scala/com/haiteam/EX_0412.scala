package com.haiteam

import org.apache.spark.sql.SparkSession

object EX_0412 {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().appName("hkproject").appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb1 = "kopo_channel_seasonality_new"
    var selloutDb2 = "kopo_product_mst"

    val selloutData1FromOracle= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb1,"user" -> staticUser, "password" -> staticPw)).load

    val selloutData2FromOracle= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb2,"user" -> staticUser, "password" -> staticPw)).load

    selloutData1FromOracle.createOrReplaceTempView("selloutTable1")
    selloutData2FromOracle.createOrReplaceTempView("selloutTable2")
    selloutData1FromOracle.show()
    selloutData2FromOracle.show()

    var middleResult = spark.sql("select " +
    //concat은 두 데이터 프레임을"_"로 이어서 합쳐주고 as를 통해 keycol로 테이터 테이블을 새로 만들어 준다.
      "concat(a.regionid,'_',a.product) as keycol, " +
      "a.regionid, " +
      "a.product, " +
      "a.yearweek, " +
      "cast(a.qty as double) as qty, " +
      "b.product_name " +
      "from selloutTable1 a " +
      "left join selloutTable2 b " +
      "on a.product = b.product_id")







  }
}

