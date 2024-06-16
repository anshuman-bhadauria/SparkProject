package Transformations

import scala.io.Source
import io.circe._
import org.apache.spark.SparkConf

object ConfigLoader extends  App {

  def getConfig(env: String): Map[String, Any] = {
    var configMap: Map[String, Any] = Map()
    val jsonFilePath = "src/test/scala/resources/conf/conf.json"
    val jsonString = Source.fromFile(jsonFilePath).getLines.mkString

    parser.parse(jsonString) match {
      case Left(failure) =>
        println(s"Failed to parse JSON: $failure")

      case Right(json) =>
        json.hcursor.get[Map[String, Json]](env) match {
          case Left(failure) =>
            println(s"Failed to find $env key: $failure")
          case Right(localConfig) =>
            configMap = localConfig.map {
              case (key, value) => (key, value)
            }
        }
    }
    configMap
  }

  def getSparkConf(env: String): SparkConf = {
    val sparkConf = new SparkConf()
    var configMap: Map[String, Any] = Map()
    val jsonFilePath = "src/test/scala/resources/conf/spark.json"
    val jsonString = Source.fromFile(jsonFilePath).getLines.mkString

    parser.parse(jsonString) match {
      case Left(failure) =>
        println(s"Failed to parse JSON: $failure")

      case Right(json) =>
        json.hcursor.get[Map[String, Json]](env) match {
          case Left(failure) =>
            println(s"Failed to find $env key: $failure")
          case Right(localConfig) =>
            configMap = localConfig.map {
              case (key, value) => (key, value)
            }
        }
    }
    configMap.map{case(key,value) => sparkConf.set(key, value.toString)}
    sparkConf
  }
}
