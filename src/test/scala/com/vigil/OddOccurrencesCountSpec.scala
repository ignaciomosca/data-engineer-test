package com.vigil

import com.vigil.Schemas._
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths
class OddOccurrencesCountSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val sourcePath = "src/test/resources/source"
  private val resultPath = "src/test/resources/result"
  private val awsProfile = "default"

  private var sc: SparkSession = _

  override def beforeAll(): Unit =
    sc = SparkSession.builder().master("local[*]").appName("OddOccurrencesCountTest").getOrCreate()

  override def afterAll(): Unit = {
    sc.stop()
    os.remove.all(os.Path(Paths.get(s"${os.pwd.toString()}/$resultPath")))
  }

  "OddOccurrencesCount" should "process source files and calculate results correctly" in {
    val expectedResultData = Array(
      KeyValue(1, 7),
      KeyValue(3, 11)
    )

    OddOccurrencesCount.main(Array(sourcePath, resultPath, awsProfile))

    // read the result data from the output files
    val resultData = sc.read
      .option("header", value = false)
      .option("delimiter", "\t")
      .schema(keyValueSchema)
      .csv(s"$resultPath/part*")
      .as[KeyValue](keyValueSchemaEncoder)
      .collect()

    resultData shouldEqual expectedResultData
  }
}
