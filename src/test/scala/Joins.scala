import Solutions.Joins._
import Utils.Read.readCsvWithOptionalSchema
import Utils.SparkUtils.{sparkContext, sparkSession}
import org.scalatest.funsuite.AnyFunSuite

/** Created By O002292
 * Date: 12/12/2023
 * Time: 16:47
 */

class Joins extends AnyFunSuite {

  private val DATA_PATH = "Joins/"

  test("innerJoin") {

    import sparkSession.implicits._

    val data_1 = Seq(("1", "a"), ("2", "b"))
    val data_2 = Seq(("1", "a"), ("3", "b"))
    val data_3 = Seq(("1", "a", "a"))

    val df_1 = sparkContext.parallelize(data_1).toDF("id", "value_1")
    val df_2 = sparkContext.parallelize(data_2).toDF("id", "value_2")
    val df_3 = sparkContext.parallelize(data_3).toDF("id", "value_1", "value_2")

    val result = innerJoin(df_1, df_2, List("id"))

    assert(result.except(df_3).isEmpty && df_3.except(result).isEmpty)
  }

  /**
   * Left Join by id
   * +---+-------+   +---+-------+ 	  +---+-------+-------+
   * | id|value_1| + | id|value_2| -> | id|value_1|value_2|
   * +---+-------+   +---+-------+	  +---+-------+-------+
   * |  1|      a|   |  1|      a|	  |  1|      a|      a|
   * |  2|      b|   |  3|      b|	  |  2|      b|   null|
   * +---+-------+   +---+-------+	  +---+-------+-------+
   */
  test("leftJoin") {

    val df_input_1 = readCsvWithOptionalSchema(s"${DATA_PATH}leftJoin_input_1")
    val df_input_2 = readCsvWithOptionalSchema(s"${DATA_PATH}leftJoin_input_2")
    val df_output  = readCsvWithOptionalSchema(s"${DATA_PATH}leftJoin_output")

    val result = leftJoin(df_input_1, df_input_2, List("col_0"))

    df_input_1.show()
    df_input_2.show()
    result.show()

    assert(result.except(df_output).isEmpty && df_output.except(result).isEmpty)
  }

  /**
   * Right Join by id
   * +---+-------+   +---+-------+ 	  +---+-------+-------+
   * | id|value_1| + | id|value_2| -> | id|value_1|value_2|
   * +---+-------+   +---+-------+	  +---+-------+-------+
   * |  1|      a|   |  1|      a|	  |  1|      a|      a|
   * |  2|      b|   |  3|      b|	  |  3|   mull|      b|
   * +---+-------+   +---+-------+	  +---+-------+-------+
   */
  test("rightJoin") {

    val df_input_1 = readCsvWithOptionalSchema(s"${DATA_PATH}leftJoin_input_1")
    val df_input_2 = readCsvWithOptionalSchema(s"${DATA_PATH}leftJoin_input_2")
    val df_output  = readCsvWithOptionalSchema(s"${DATA_PATH}leftJoin_output")

    val result = rightJoin(df_input_1, df_input_2, List("col_0"))

    df_input_1.show()
    df_input_2.show()
    result.show()

    assert(result.except(df_output).isEmpty && df_output.except(result).isEmpty)
  }
}
