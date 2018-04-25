package com.haiteam.Example_Seasonality

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Exaple_0425 {
  def main(args: Array[String]): Unit = {

    var spark = SparkSession.builder().config("spark.master", "local").getOrCreate()



   //2.번 문제

    //접속정보
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_seasonality_new"

    //jdbc 연결하여 자료 로딩 => 데이터프레임에 저장
    val selloutDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb,
        "user" -> staticUser,
        "password" -> staticPw)).load

    //데이터 프레임확인
    selloutDf.show(2);

    //메모리테이블 생성
    selloutDf.createOrReplaceTempView("selloutTable")

    //spark SQL로 자료 셀렉트
    var rawData = spark.sql("select " +
      "regionid, " +
      "product, " +
      "yearweek, " +
      "qty, " +
      "qty*1.2 as qty_New from selloutTable "
      )


    //5번문제

    var rawDataColumns = rawData.columns
    var regionidNo = rawDataColumns.indexOf("regionid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    var qtyNo = rawDataColumns.indexOf("qty")
    var qtyNewNo = rawDataColumns.indexOf("qty_New")

    var rawRdd = rawData.rdd

    var productArray = Array("PRODUCT1","PRODUCT2")
    //세트 타입으로 변환
    var productSet = productArray.toSet

    var filteredRdd = rawRdd.filter(x => {
      var checkValid = false
      var yearVlaue = (x.getString(yearweekNo).substring(0, 4)).toInt
      var weekvalue = (x.getString(yearweekNo).substring(4)).toInt
      var productInfo = x.getString(productNo)

      //condition 2016년도 이산, 53주차 제외,(product 1, product 2) 포함
      if ((yearVlaue >= 2016 && weekvalue != 53) &&
        productSet.contains(productInfo)) {
      checkValid = true
      }
    checkValid
    })


    // scara 에서도 import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType} 임포트 해줘야 함

    val finalResultDf = spark.createDataFrame(filteredRdd,
      StructType(
        Seq(
          StructField("REGIONID", StringType),
          StructField("PRODUCT", StringType),
          StructField("YEARWEEK", StringType),
          StructField("qty", DoubleType),
          StructField("qty_New", DoubleType))))



}


