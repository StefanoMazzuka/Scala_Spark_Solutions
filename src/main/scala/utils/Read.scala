/** Created By O002292
 * Date: 20/11/2023
 * Time: 12:16
 */

package utils

import utils.Characters.DELIMITER_TYPE
import utils.Constants.{CSV_EXTENSION, TEST_DATA_PATH}
import utils.Options.{DELIMITER, HEADER}
import utils.SparkUtils.{sparkContext, sparkSession, stopSpark}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType, StructType}

object Read {

  def main(args: Array[String]): Unit = {
    val df_0 = readCsvWithOptionalSchema("data_01")
    df_0.printSchema()
    /*
      root
        |-- col_0: string (nullable = true)
        |-- col_1: string (nullable = true)
        |-- col_2: string (nullable = true)
     */
    df_0.show()
    /*
      +-----+-----+-----+
      |col_0|col_1|col_2|
      +-----+-----+-----+
      |  t_1|    1|  1.1|
      |  t_2|    2|  2.2|
      |  t_3|    3|  3.3|
      |  t_4|    4|  4.4|
      +-----+-----+-----+
     */

    val schema = new StructType()
      .add("col_0", StringType, true)
      .add("col_1", IntegerType, true)
      .add("col_2", DecimalType(2,1), true)
    val df_1 = readCsvWithOptionalSchema("data_01", schema)
    df_1.printSchema()
    /*
      root
        |-- col_0: string (nullable = true)
        |-- col_1: integer (nullable = true)
        |-- col_2: decimal(2,1) (nullable = true)
     */
    df_1.show()
    /*
      +-----+-----+-----+
      |col_0|col_1|col_2|
      +-----+-----+-----+
      |  t_1|    1|  1.1|
      |  t_2|    2|  2.2|
      |  t_3|    3|  3.3|
      |  t_4|    4|  4.4|
      +-----+-----+-----+
     */

    createDataFrameWithSchema.show()

    stopSpark
  }

  /**
   * Creates a dataframe from a csv by optionally applying an unofficial schema to it
   * @param csv_name Name of the csv located in src/main/resources/csv/
   * @param schema Optional schema
   * @return Dataframe
   */
  def readCsvWithOptionalSchema(csv_name: String, schema: StructType = new StructType()): DataFrame = {

    val base_reader = sparkSession
      .read
      .option(HEADER, true)
      .option(DELIMITER, DELIMITER_TYPE)

    if (schema.isEmpty) base_reader.csv(TEST_DATA_PATH + csv_name + CSV_EXTENSION)
    else base_reader.schema(schema).csv(TEST_DATA_PATH + csv_name + CSV_EXTENSION)
  }

  def createDataFrameWithSchema: DataFrame = {
    val data = Seq(
      Row("Paco", "Garcia", 20),
      Row("Juan", "Garcia", 26),
      Row("Lola", "Martin", 29),
      Row("Sara", "Martin", 35),
      Row("Paola", "Martin", 30)
    )
    val rdd = sparkSession.sparkContext.parallelize(data)
    val schema = new StructType()
      .add("col_0", StringType, true)
      .add("col_1", StringType, true)
      .add("col_2", IntegerType, true)

    val df = sparkSession.createDataFrame(rdd, schema)

    df
  }

  def createDataFrameWithoutSchema: DataFrame = {
    import sparkSession.implicits._
    val data = Seq(("a", "a"), ("b", "b"))
    val df = sparkContext.parallelize(data).toDF("col_0", "col_1")
    df
  }
}
