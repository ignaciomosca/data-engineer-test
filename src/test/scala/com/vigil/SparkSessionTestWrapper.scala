package com.vigil

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }

}
