import Solutions.SelectColumns.newSpecialColumn
import Utils.Read.readCsvWithOptionalSchema
import org.scalatest.funsuite.AnyFunSuite

/** Created By O002292
 * Date: 15/12/2023
 * Time: 10:04
 */

class SelectColumns extends AnyFunSuite {

  private val DATA_PATH = "SelectColumns/"

  test("methodName") {

    val df_input  = readCsvWithOptionalSchema(s"${DATA_PATH}newSpecialColumn_input")
    val df_output = readCsvWithOptionalSchema(s"${DATA_PATH}newSpecialColumn_output")
    val df_result = newSpecialColumn(df_input)

    df_input.show()
    df_result.show()

    assert(df_result.except(df_output).isEmpty && df_output.except(df_result).isEmpty)
  }

}
