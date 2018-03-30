package com.haiteam

object Ex0330 {



  package com.haiteam

  import org.apache.spark.sql.SparkSession



      var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
      var staticUser = "kopo"
      var staticPw = "kopo"
      var selloutDb = "kopo_channel_seasonality_ex"

      val selloutDataFromOracle= spark.read.format("jdbc").
        options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

      selloutDataFromOracle.createOrReplaceTempView("selloutTable")
      selloutDataFromOracle.show()
    }
  }