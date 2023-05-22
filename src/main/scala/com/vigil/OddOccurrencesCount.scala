package com.vigil

import com.vigil.Schemas.keyValueSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum}

object OddOccurrencesCount {
  def main(args: Array[String]): Unit = {

    val inputS3Path = args(0)
    val outputS3Path = args(1)
    val awsProfile = args(2)

    val sc = SparkSession
      .builder()
      .master("local[*]") // run Spark locally with as many worker threads as logical cores on the machine.
      .appName("OddOccurrencesCount")
      .getOrCreate()

    // set credentials for S3 access
    // val credentials = Credentials.fetchCredentials(awsProfile)
    // sc.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.getCredentials.getAWSAccessKeyId)
    // sc.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.getCredentials.getAWSSecretKey)

    sc
      .read
      .option("header", value = true)
      .option("delimiter", "\t")
      .schema(keyValueSchema)
      .csv(inputS3Path)
      .na
      .fill(0) // The empty string should represent 0
      .groupBy("key")
      .agg(sum("value").alias("result")) // sum the values for each key and put them in the result column
      .where(col("result") % 2 =!= 0) // filter out even results
      .select("key", "result")
      .write
      .option("delimiter", "\t")
      .csv(outputS3Path)
  }

}
