package com.haiteam

import org.apache.spark.sql.SparkSession
import com.hk
object Example_5 {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().appName("hkproject").
      config("spark.master", "local").
      getOrCreate()

    ///////////////////////////     Postgres / GreenplumDB 데이터 로딩 ////////////////////////////////////
    // 접속정보 설정
    var staticUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_batch_season_mpara"

    // jdbc (java database connectivity) 연결
    val selloutDataFromPg= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
    selloutDataFromPg.createOrReplaceTempView("selloutTable")

    var outputUrl = "jdbc:oracle:thin:@192.168.110.19:1522/XE"
    var outputUser = "DWKIM"
    var outputPw = "dwkim"


    // 데이터 저장
    var prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", outputUser)
    prop.setProperty("password", “outputPw)
    var table = "kopo_channel_seasonalit"
    //append
    selloutDataFromPg.write.mode("overwrite").jdbc(outputUrl, table, prop)




  }
}
