package com.vigil

import com.github.mrpowers.spark.fast.tests.ColumnComparer
import org.apache.spark.sql.functions.col
import org.scalatest.wordspec.AnyWordSpec

class FunctionsSpec extends AnyWordSpec with SparkSessionTestWrapper with ColumnComparer {

  import spark.implicits._
  "isEven" should {

    "returns true if the number is even and false otherwise" in {

      val data = Seq(
        (1, false),
        (2, true),
        (3, false)
      )

      val df = data.toDF("some_num", "expected").withColumn("actual", functions.isEven(col("some_num")))

      assertColumnEquality(df, "actual", "expected")

    }

  }

}
