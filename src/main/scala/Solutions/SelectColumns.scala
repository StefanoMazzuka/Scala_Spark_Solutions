/** Created By O002292
 * Date: 20/11/2023
 * Time: 13:24
 */

package Solutions

import Utils.Columns.{COL_RESULT, COL_WITH_VALUE, FIRST_NOT_NULL_COL}
import Utils.Read.readCsvWithOptionalSchema
import Utils.SparkUtils.{sparkContext, sparkSession}
import org.apache.spark.sql.functions.{coalesce, col, lit, when}
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row}

import javax.crypto.Mac

object SelectColumns {

  def main(args: Array[String]): Unit = {
    val schema = new StructType()
      .add("col_0", StringType, true)
      .add("col_1", IntegerType, true)
      .add("col_2", DecimalType(2,1), true)
    val df = readCsvWithOptionalSchema("data_02", schema)
    val new_rows = Seq(
      Map("col_1" -> "5"),
      Map("col_0" -> "value2", "col_2" -> "2.2")
    )

    addRowsToDataFrame(df, new_rows).show()
  }

  /**
   * Add a new row to a DataFrame
   * @param df DataFrame to modify
   * @param rows Rows you want to add.
   * Example:
   * val new_rows = Seq(
   *    Map("col_1" -> "2.2"),
   *    Map("col_1" -> "2", "col_2" -> "value2")
   * )
   * @return DataFrame with a new row
   */
  def addRowsToDataFrame(df: DataFrame, rows: Seq[Map[String, String]]): DataFrame = {

    val columns     = rows.flatMap({m => m.keys}).distinct
    val schema      = StructType(columns.map(col_name => StructField(col_name, StringType, true)))
    val data        = rows.map(row => Row.fromSeq(schema.map(field => row.getOrElse(field.name, null))))
    val rdd         = sparkContext.parallelize(data)
    val df_new_rows = sparkSession.createDataFrame(rdd, schema)

    df.unionByName(df_new_rows, true)
  }

  /**
   * Get first not null element in any row
   * @param df
   * @return
   */
  def newSpecialColumn(df: DataFrame): DataFrame = {

    val special_cols   = List("col_0", "col_1", "col_2", "col_3")
    val new_col_result = when(coalesce(special_cols.map(col):_*) === 1 || coalesce(special_cols.map(col):_*) === 2, col(COL_WITH_VALUE))
      .otherwise(lit(0)).as(COL_RESULT)
    val final_cols     = df.columns.map(col).toBuffer -= col(COL_RESULT) += new_col_result

    df.select(final_cols:_*)
  }
}
