package com.haiteam

import com.haiteam.Example_join.spark
import org.apache.spark.sql.SparkSession;

object Example_0405 {

  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().appName("hkproject").appName("hkProject").
    config("spark.master", "local").
    getOrCreate()

    //oracle 데이터 로딩////////////
    //접속정보설정
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb1 = "kopo_channel_seasonality_new"
    var selloutDb2 = "kopo_region_mst"
//jdbc 연결
  val selloutData1= spark.read.format("jdbc").
    options(Map("url" -> staticUrl,"dbtable" -> selloutDb1,"user" -> staticUser, "password" -> staticPw)).load
    val selloutData2= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb2,"user" -> staticUser, "password" -> staticPw)).load

    //메모리테이블 생성
   selloutData1.createOrReplaceTempView("selloutTable1")
    selloutData2.createOrReplaceTempView("selloutTable2")

    //inner join
    var innerjoinData= spark.sql("select a.regionid, b.regionname, a.product, a.yearweek, a.qty " +
      "from selloutTable1 a " +
      "inner join selloutTable2 b " +
      "on a.regionid = b.regionid")
}
//left join

var leftjoinData=spark.sql("select a.regionid, b.regionname, a.product, a.yearweek, a.qty " +
"from selloutTable1 a " +
"left join selloutTable2 b " +
"on a.regionid = b.regionid")
}


