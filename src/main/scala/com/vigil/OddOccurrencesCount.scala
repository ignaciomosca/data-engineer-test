package com.vigil

import com.vigil.Schemas.{keyValueSchema, keyValueSchemaEncoder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OddOccurrencesCount {

  def main(args: Array[String]): Unit = {

    val inputS3Path = args(0)
    val outputS3Path = args(1)
    val awsProfile = args(2)

    val sc = SparkSession
      .builder()
      .master("local[*]") // run Spark locally with as many worker threads as logical cores on the machine.
      .appName("OddOccurrencesCount")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
      .config("spark.hadoop.fs.s3a.aws.profile", awsProfile)
      .getOrCreate()

    // set credentials for S3 access
    val credentials = Credentials.fetchCredentials(awsProfile)
    sc.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", credentials.getCredentials.getAWSAccessKeyId)
    sc.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", credentials.getCredentials.getAWSSecretKey)
    sc.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")

    //dataframeSolution(inputS3Path, outputS3Path, sc)
    sparkSQL(inputS3Path, outputS3Path, sc)

  }

  private def dataframeSolution(inputS3Path: String, outputS3Path: String, sc: SparkSession): Unit = {
    // Read the input files as a DataFrame
    val inputDF = sc.read
      .option("header", "true")
      .schema(keyValueSchema)
      .csv(inputS3Path)

    // Replace empty strings with 0 in the value column
    val cleanedDF = inputDF.withColumn("value", when(col("value") === "", 0).otherwise(col("value")))

    // Group by key and value, and count the occurrences
    val countsDF = cleanedDF.groupBy("key", "value").count()

    // Filter out values with even counts
    val oddCountsDF = countsDF.filter(col("count") % 2 =!= 0)

    // Select the key and value columns
    val resultDF = oddCountsDF.select("key", "value")

    // Write the result DataFrame as TSV to the output S3 path
    resultDF.write
      .option("header", "false")
      .option("delimiter", "\t")
      .csv(outputS3Path)
  }

  private def sparkSQL(inputS3Path: String, outputS3Path: String, sc: SparkSession): Unit = {
    // Read the input files as a DataFrame
    val inputDF = sc.read
      .option("header", "true")
      .schema(keyValueSchema)
      .csv(inputS3Path)

    // Register a temporary view for SQL queries
    inputDF.createOrReplaceTempView("data")

    // SQL query to find the odd value occurrences for each key
    val resultDF = sc.sql(
      """
        |SELECT key, value
        |FROM (
        |  SELECT key, value, COUNT(*) AS count
        |  FROM data
        |  GROUP BY key, value
        |) tmp
        |WHERE count % 2 != 0
       """.stripMargin)

    // Write the result DataFrame as CSV/TSV to the output S3 path
    resultDF.as(keyValueSchemaEncoder).write
      .option("header", "false")
      .option("delimiter", "\t")
      .csv(outputS3Path)
  }
}
