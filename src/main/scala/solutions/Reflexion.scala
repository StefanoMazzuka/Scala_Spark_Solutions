/** Created By O002292
 * Date: 15/03/2024
 * Time: 13:19
 */

package solutions

import org.apache.spark.sql.DataFrame
import utils.Read.createDataFrameWithSchema

import scala.tools.reflect.ToolBox

object Reflexion {

  def main(args: Array[String]): Unit = {
    val df = createDataFrameWithSchema
    df.show()

    // Read input from the user
    print("Set your function: ")
    val input_function: String = scala.io.StdIn.readLine() // df.filter(col("col_0") === "Sara") df.withColumn("new_col", lit("value"))

    customFunction(df, input_function).show()
  }

  private def customFunction(df: DataFrame, input_function: String): DataFrame = {
    // Crear una instancia de ToolBox
    val toolbox = scala.reflect.runtime.currentMirror.mkToolBox()

    // Definir el string que contiene la expresión, incluyendo los imports necesarios
    val expression_string =
      s"""
         |import org.apache.spark.sql._
         |import org.apache.spark.sql.functions._
         |import scala.reflect.runtime.{universe => ru}
         |{ df: DataFrame =>
         |  $input_function
         |}
       """.stripMargin

    // Convertir el string a una expresión utilizando el ToolBox
    val expression = toolbox.eval(toolbox.parse(expression_string)).asInstanceOf[DataFrame => DataFrame]

    // Ejecutar la expresión con el DataFrame
    expression(df)
  }
}
