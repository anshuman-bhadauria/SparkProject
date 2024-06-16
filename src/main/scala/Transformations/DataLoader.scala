package Transformations

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}


object DataLoader {

  private def getAccountSchema: String ={
    """load_date date,active_ind int,account_id string,source_sys string,account_start_date timestamp,
       legal_title_1 string,legal_title_2 string,tax_id_type string,tax_id string,branch_code string,country string"""
  }

  private def getPartySchema: String = {
    """load_date date,account_id string,party_id string,relation_type string,relation_start_date timestamp"""
  }

  private def getAddressSchema: String = {
    """load_date date,party_id string,address_line_1 string,address_line_2 string,city string,postal_code string,
       country_of_address string,address_start_date date"""
  }

  def readAccountData( spark : SparkSession, env:String, enable_hive:Boolean, hive_db:String ) : DataFrame ={
    if(enable_hive){
      spark.sql("select * from "+ hive_db + ".accounts").where(col("active_ind") === 1)
    }else{
      spark.read.format("csv")
      .option("header", "true")
      .schema(getAccountSchema)
      .load("src/test/scala/resources/test_data/accounts/account_samples.csv")
      .where(col("active_ind") === 1)
    }
  }

  def readPartyData(spark : SparkSession, env: String, enable_hive: Boolean, hive_db: String): DataFrame = {

    if (enable_hive) {
      spark.sql("select * from " + hive_db + ".parties")
    } else {
      spark.read.format("csv")
        .option("header", "true")
        .schema(getPartySchema)
        .load("src/test/scala/resources/test_data/parties/party_samples.csv")
    }
  }

  def readAddressData(spark : SparkSession, env: String, enable_hive: Boolean, hive_db: String): DataFrame = {
    if (enable_hive) {
      spark.sql("select * from " + hive_db + ".address")
    } else {
      spark.read.format("csv")
        .option("header", "true")
        .schema(getAddressSchema)
        .load("src/test/scala/resources/test_data/party_address/address_samples.csv")
    }
  }

}
