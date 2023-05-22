package com.vigil

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.wordspec.AnyWordSpec

class TransformsSpec extends AnyWordSpec with SparkSessionTestWrapper with DataFrameComparer {

  import spark.implicits._

  ".happyData" should {

    "appends a happy column to a DataFrame" in {

      val sourceDF = Seq(
        ("jose"),
        ("li"),
        ("luisa")
      ).toDF("name")

      val actualDF = sourceDF.transform(transforms.happyData())

      val expectedData = List(
        Row("jose", "data is fun"),
        Row("li", "data is fun"),
        Row("luisa", "data is fun")
      )

      val expectedSchema = List(
        StructField("name", StringType, true),
        StructField("happy", StringType, false)
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)

    }

  }

}
