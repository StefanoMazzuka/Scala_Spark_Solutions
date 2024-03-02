/** Created By O002292
 * Date: 29/11/2023
 * Time: 11:57
 */

package solutions

import utils.Read.readCsvWithOptionalSchema
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object Aggregations {

  def main(args: Array[String]): Unit = {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("col_0", StringType)
    val df     = readCsvWithOptionalSchema("data_03", schema)
    val max_id = df.agg(max("id")).first()(0).toString.toInt
    println((max_id + 1).toString)
  }
}
