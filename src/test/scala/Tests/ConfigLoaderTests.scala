package Tests

import Transformations.ConfigLoader.getConfig
import Transformations.Utils.getSparkSession
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class ConfigLoaderTests extends AnyFunSuite {

  val spark: SparkSession={
    getSparkSession("LOCAL")
  }

  test("Test Spark Version"){
    spark.version shouldBe("3.5.1")
  }

  test("Test Config Loader for different Environments "){
    val confLocal = getConfig("LOCAL")
    val confProd = getConfig("PROD")

    confLocal("enable.hive").toString shouldBe "false"
    confProd("enable.hive").toString shouldBe "true"
  }
}
