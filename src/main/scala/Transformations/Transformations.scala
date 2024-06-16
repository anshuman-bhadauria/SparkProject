package Transformations

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Transformations {

  def getInsertOperations(column : Column, name : String): Column= {
     struct(lit("INSERT").alias("operation"),
            column.as("newValue"),
            lit(None).as("oldValue")).alias(name)
  }

  def getContract(df:DataFrame):DataFrame ={
    val contract_title = array(when(col("legal_title_1").isNotNull,
                                struct(lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
                                       col("legal_title_1").alias("contractTitleLine")
                                       ).alias("contractTitle")),
                                when(col("legal_title_2").isNotNull,
                                struct(lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
                                       col("legal_title_2").alias("contractTitleLine")
                                      ).alias("contractTitle"))
                             )

    val contract_title_nl = not(isnull(contract_title))

    val tax_identifier = struct(col("tax_id_type").alias("taxIdType"),
                                col("tax_id").alias("taxId")
                                ).alias("taxIdentifier")

    df.select(
      col("account_id"),
      getInsertOperations(col("account_id"), "contractIdentifier"),
      getInsertOperations(col("source_sys"), "sourceSystemIdentifier"),
      getInsertOperations(col("account_start_date"), "contactStartDateTime"),
      getInsertOperations(contract_title_nl, "contractTitle"),
      getInsertOperations(tax_identifier, "taxIdentifier"),
      getInsertOperations(col("branch_code"), "contractBranchCode"),
      getInsertOperations(col("country"), "contractCountry"),
    )
  }

  def getRelations(df : DataFrame):DataFrame = {
    df.select(col("account_id"),
              col("party_id"),
              getInsertOperations(col("party_id"), "partyIdentifier"),
              getInsertOperations(col("relation_type"), "partyRelationType"),
              getInsertOperations(col("relation_start_date"), "partyRelationStartDate")
             )
  }

  def getAddress(df: DataFrame): DataFrame = {

    val address = struct(col("address_line_1").alias("addressLine1"),
                  col("address_line_2").alias("addressLine2"),
                  col("city").alias("addressCity"),
                  col("postal_code").alias("addressPostalCode"),
                  col("country_of_address").alias("addressCountry"),
                  col("address_start_date").alias("addressStartDate"))

    df.select(col("party_id"),
      getInsertOperations(address, "partyAddress")
    )
  }

  def joinPartyAddress(party : DataFrame, address : DataFrame) : DataFrame = {
    party.join(
      address, "party_id", "left_outer")
    .groupBy(col("account_id"))
    .agg(collect_list(struct(col("partyIdentifier"),
                             col("partyRelationshipType"),
                             col("partyRelationStartDateTime"),
                             col("partyAddress")
    ).alias("partyDetails")
    ).alias("partyRelations")
    )
  }

  def joinContractParty(contract : DataFrame, party : DataFrame): DataFrame = {
    contract.join(party, "account_id", "left_outer")
  }

  def applyHeader(spark : SparkSession, df : DataFrame): DataFrame ={
    val header_info = Seq(("SBDL-Contract",1,0))

    val header_df = spark.createDataFrame(header_info).toDF("eventType", "majorSchemaVersion", "minorSchemaVersion")


    header_df.hint("broadcast").crossJoin(df)
    .select(
      struct(expr("uuid()").alias("eventIdentifier"),
             col("eventType"),
             col("majorSchemaVersion"),
             col("minorSchemaVersion"),
             lit(date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssZ")).alias("eventDateTime")
             ).alias("eventHeader"),
             array(struct(lit("contractIdentifier").alias("keyField"),
                   col("account_id").alias("keyValue")
             )).alias("keys"),
             struct(col("contractIdentifier"),
                    col("sourceSystemIdentifier"),
                    col("contactStartDateTime"),
                    col("contractTitle"),
                    col("taxIdentifier"),
                    col("contractBranchCode"),
                    col("contractCountry"),
                    col("partyRelations")
             ).alias("payload")
    )
  }



}
