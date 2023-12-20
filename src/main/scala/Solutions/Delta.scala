/** Created By O002292
 * Date: 22/11/2023
 * Time: 12:52
 */

package Solutions

import Utils.Characters.DELIMITER_TYPE
import Utils.Constants.{LOCAL_HDFS_PATH, TEST_DATA_PATH}
import Utils.Options.{DELIMITER, HEADER}
import Utils.Read.readCsvWithOptionalSchema
import Utils.SparkUtils.{sparkContext, sparkSession}
import io.delta.tables.DeltaTable
import org.apache.hadoop.util.Time
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{Decimal, DecimalType, IntegerType, StringType, StructField, StructType}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter.ofPattern

object Delta {

  def main(args: Array[String]): Unit = {
    createDelta()
    println(sparkSession.version)
  }

  def createDelta(): Unit = {

    val now = ofPattern("yyyy-MM-dd HH:mm:ss.ss").format(LocalDateTime.now)

    val data = Seq(
      Row("AA1", "ES", Decimal("202310"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "ES", Decimal("201201"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "ES", Decimal("200808"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "ES", Decimal("201909"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "ES", Decimal("202304"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "ES", Decimal("202005"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "AR", Decimal("202201"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "AR", Decimal("200201"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "AR", Decimal("201204"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "AR", Decimal("202103"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "AR", Decimal("202301"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "AR", Decimal("201602"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "MX", Decimal("201902"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "MX", Decimal("202201"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "MX", Decimal("201709"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "MX", Decimal("201710"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "MX", Decimal("202201"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "MX", Decimal("201105"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "MX", Decimal("201610"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "MX", Decimal("201610"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "MX", Decimal("201610"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "MX", Decimal("201610"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "MX", Decimal("201610"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "MX", Decimal("201610"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"),
      Row("AA1", "MX", Decimal("201610"), Decimal("368263.234"), Decimal("97375123.342"), Decimal("192734734.234"), Decimal("234.56"), "Gatito", now, "20121130"))

    val rdd = sparkSession.sparkContext.parallelize(data)

    val schema = new StructType()
    .add("gf_ucm_cell_id", StringType, true)
    .add("g_entific_id", StringType, true)
    .add("gf_year_month_date_id", DecimalType(6, 0), true)
    .add("gf_cell_calc_amount", DecimalType(28, 6), true)
    .add("gf_lt_calc_bal_cell_amount", DecimalType(28, 6), true)
    .add("gf_dif_bt_calc_cells_amount", DecimalType(28, 6), true)
    .add("gf_dif_bt_calc_cells_per",DecimalType(12, 9), true)
    .add("gf_ucm_cell_desc", StringType, true)
    .add("gf_audit_date", StringType, true)
    .add("gf_odate_date_id", StringType, true)

    val df = sparkSession.createDataFrame(rdd, schema)

    df.show()

    df.write
      .mode(SaveMode.Overwrite)
      .format("delta")
      .partitionBy("g_entific_id", "gf_year_month_date_id")
      .save("C:\\Users\\O002292\\Desktop\\local_hdfs\\data\\master\\krlt\\data\\t_krlt_finrep_contrast")

    val df_2 = sparkSession
      .read
      .option(HEADER, true)
      .option(DELIMITER, DELIMITER_TYPE)
      .csv("C:\\Users\\O002292\\Desktop\\local_hdfs\\csv\\t_krlt_osi_protection_pipeline.csv")

    df_2.write
      .mode(SaveMode.Overwrite)
      .format("delta")
      .partitionBy("gf_osi_id", "gf_osi_adj_ind_type", "g_entific_id", "gf_portfolio_id", "gf_cutoff_date")
      .save("C:\\Users\\O002292\\Desktop\\local_hdfs\\data\\master\\krlt\\data\\t_krlt_osi_protection")
  }
  def deleteDeltaPartition(delta: DeltaTable): DeltaTable = {
    ???
  }
}
