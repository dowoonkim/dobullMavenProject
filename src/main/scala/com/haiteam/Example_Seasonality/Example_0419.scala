package com.haiteam.Example_Seasonality

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, DoubleType, StructField, StructType}

object Example_0419 {

  def main(args: Array[String]): Unit = {

    var spark = SparkSession.builder().config("spark.master", "local").getOrCreate()

    // oracle connection
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"

    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"
    var productNameDb = "kopo_product_mst"

    val selloutDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb,
        "user" -> staticUser,
        "password" -> staticPw)).load

    val productMasterDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> productNameDb,
        "user" -> staticUser,
        "password" -> staticPw)).load

    selloutDf.createOrReplaceTempView("selloutTable")
    productMasterDf.createOrReplaceTempView("mstTable")

    var rawData = spark.sql("select " +
      "concat(a.regionid,'_',a.product) as keycol, " +
      "a.regionid as accountid, " +
      "a.product, " +
      "a.yearweek, " +
      "cast(a.qty as double) as qty, " +
      "b.product_name " +
      "from selloutTable a " +
      "left join mstTable b " +
      "on a.product = b.product_id")

    rawData.show(2)

    var rawDataColumns = rawData.columns
    var keyNo = rawDataColumns.indexOf("keycol")
    var accountidNo = rawDataColumns.indexOf("accountid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    var qtyNo = rawDataColumns.indexOf("qty")
    var productnameNo = rawDataColumns.indexOf("productname")

    var rawRdd = rawData.rdd



    var filteredRdd= rawRdd.filter(x=> {
      //boolean = true
      var checkValid=true
      // 찾기: yearweek인덱스로 주차정보만 인트타입으로 변환
      var weekValue = x.getString(yearweekNo).substring(4).toInt

      //비교한 후 주차정보가 53이상인 경우 레코드 삭제
      if(weekValue >= 53){
        checkValid=false
      }
      checkValid
    })


    //상품정보가 product 1,2 인 정보만 필터링
    //분석대상 제품군 등록
    var productArray = Array("PRODUCT1","PRODUCT2")
    //세트 타입으로 변환
    var productSet = productArray.toSet

    var resultRdd = filteredRdd.filter(x=>{
      var checkValid = false

      //데이터 특정 행의 product 컬럼인덱스를 활용하여 데이터 대입
      var productInfo = x.getString(productNo)

      if(productSet.contains(productInfo)){
        checkValid = true
      }

      //2번째 (근데 이거는 코드가 너무 지저분해져! 추천 ㄴㄴ)
      if((productInfo == "PRODUCT1") || (productInfo == "PRODUCT2") ){
        checkValid = true
      }
      checkValid
    })

    val finalResultDf = spark.createDataFrame(resultRdd,
      StructType(
        Seq(
          StructField("KEY", StringType),
          StructField("REGIONID", StringType),
          StructField("PRODUCT", StringType),
          StructField("YEARWEEK", StringType),
          StructField("VOLUME", DoubleType),
          StructField("PRODUCT_NAME", StringType))))




  }

}