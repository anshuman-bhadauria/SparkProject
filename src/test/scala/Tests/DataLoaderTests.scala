package Tests

import Transformations.DataLoader
import Transformations.Utils.getSparkSession
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{Date, Timestamp}

class DataLoaderTests extends AnyFunSuite{
  val spark: SparkSession = {
    getSparkSession("LOCAL")
  }

  case class PartyInfo(load_date: Date, account_id: String, party_id: String, relation_type: String, relation_start_date: Timestamp )

  val expectedPartyRows: Seq[Row] = Seq(
    PartyInfo(Date.valueOf("2022-08-02"), "6982391060", "9823462810", "F-N", Timestamp.valueOf("2019-07-29 06:21:32")),
    PartyInfo(Date.valueOf("2022-08-02"), "6982391061", "9823462811", "F-N", Timestamp.valueOf("2018-08-31 05:27:22")),
    PartyInfo(Date.valueOf("2022-08-02"), "6982391062", "9823462812", "F-N", Timestamp.valueOf("2018-08-25 15:50:29")),
    PartyInfo(Date.valueOf("2022-08-02"), "6982391063", "9823462813", "F-N", Timestamp.valueOf("2018-05-11 07:23:28")),
    PartyInfo(Date.valueOf("2022-08-02"), "6982391064", "9823462814", "F-N", Timestamp.valueOf("2019-06-06 14:18:12")),
    PartyInfo(Date.valueOf("2022-08-02"), "6982391065", "9823462815", "F-N", Timestamp.valueOf("2019-05-04 05:12:37")),
    PartyInfo(Date.valueOf("2022-08-02"), "6982391066", "9823462816", "F-N", Timestamp.valueOf("2019-05-15 10:39:29")),
    PartyInfo(Date.valueOf("2022-08-02"), "6982391067", "9823462817", "F-N", Timestamp.valueOf("2018-05-16 09:53:04")),
    PartyInfo(Date.valueOf("2022-08-02"), "6982391068", "9823462818", "F-N", Timestamp.valueOf("2017-11-27 01:20:12")),
    PartyInfo(Date.valueOf("2022-08-02"), "6982391067", "9823462820", "F-S", Timestamp.valueOf("2017-11-20 14:18:05")),
    PartyInfo(Date.valueOf("2022-08-02"), "6982391067", "9823462821", "F-S", Timestamp.valueOf("2018-07-19 18:56:57"))
  ).map(info => Row(info.load_date, info.account_id, info.party_id, info.relation_type, info.relation_start_date))

  test("Read Account Table Data ") {
    val accounts_df = DataLoader.readAccountData(spark, "LOCAL", false, null)
    assert(accounts_df.count() == 8)
  }

}
