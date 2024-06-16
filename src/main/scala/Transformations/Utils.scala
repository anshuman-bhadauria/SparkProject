package Transformations

import ConfigLoader.{getSparkConf}
import org.apache.spark.sql.SparkSession

object Utils {
  def getSparkSession(env : String): SparkSession= {
      if(env.equals("LOCAL")){
        SparkSession.builder
        .config(conf = getSparkConf(env))
        .config("spark.sql.autoBroadcastJoinThreshold", -1)
        .config("spark.sql.adaptive.enabled",false)
        .config("spark.driver.extraJavaOptions", "- Dlog4j.configuration = file: log4j.properties")
        .master("local[2]")
        .getOrCreate()
      }else{
        SparkSession.builder
          .config(conf = getSparkConf(env))
          .master("local[2]")
          .enableHiveSupport()
          .getOrCreate()
      }
  }

}
