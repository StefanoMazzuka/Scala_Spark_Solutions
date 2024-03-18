/** Created By O002292
 * Date: 15/03/2024
 * Time: 13:19
 */

package solutions

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import utils.Read.createDataFrameWithSchema
import utils.SparkUtils.sparkSession

import scala.tools.reflect.ToolBox

object Reflexion {

  def main(args: Array[String]): Unit = {
//    val df = createDataFrameWithSchema
//    df.show()

    // Read input from the user
    print("Set your function: ")
    val read_function: String = "    val data = Seq(\n      Row(\"Paco\", \"Garcia\", 20),\n      Row(\"Juan\", \"Garcia\", 26),\n      Row(\"Lola\", \"Martin\", 29),\n      Row(\"Sara\", \"Martin\", 35),\n      Row(\"Paola\", \"Martin\", 30)\n    )\n    val rdd = sparkSession.sparkContext.parallelize(data)\n    val schema = new StructType()\n      .add(\"col_0\", StringType, true)\n      .add(\"col_1\", StringType, true)\n      .add(\"col_2\", IntegerType, true)\n\n    sparkSession.createDataFrame(rdd, schema)"
    val input_function: String = "df.withColumn(\"new_col\", lit(\"value\"))" // df.filter(col("col_0") === "Sara") df.withColumn("new_col", lit("value"))
    val join_function: String = "df_1.join(df_2, \"new_col\", \"left\")"

    val df = read(sparkSession, "", read_function)
    val df_1 = customFunction(df, input_function)
    customJoin(df_1, df_1, join_function).show()
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

  private def read(sparkSession: SparkSession, path: String, read_function: String): DataFrame = {
    // Crear una instancia de ToolBox
    val toolbox = scala.reflect.runtime.currentMirror.mkToolBox()

    // Definir el string que contiene la expresión, incluyendo los imports necesarios
    val expression_string =
      s"""
         |import org.apache.spark.sql._
         |import org.apache.spark.sql.functions._
         |import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
         |import scala.reflect.runtime.{universe => ru}
         |{ (sparkSession: SparkSession, path: String) =>
         |  $read_function
         |}
       """.stripMargin

    // Convertir el string a una expresión utilizando el ToolBox
    val expression = toolbox.eval(toolbox.parse(expression_string)).asInstanceOf[(SparkSession, String) => DataFrame]

    // Ejecutar la expresión con el DataFrame
    expression(sparkSession, path)
  }

  private def write(df: DataFrame): Unit = {

  }

  private def customJoin(df_1: DataFrame, df_2: DataFrame, input_function: String): DataFrame = {
    // Crear una instancia de ToolBox
    val toolbox = scala.reflect.runtime.currentMirror.mkToolBox()

    // Definir el string que contiene la expresión, incluyendo los imports necesarios
    val expression_string =
      s"""
         |import org.apache.spark.sql._
         |import org.apache.spark.sql.functions._
         |import scala.reflect.runtime.{universe => ru}
         |{ (df_1: DataFrame, df_2: DataFrame) =>
         |  $input_function
         |}
       """.stripMargin

    // Convertir el string a una expresión utilizando el ToolBox
    val expression = toolbox.eval(toolbox.parse(expression_string)).asInstanceOf[(DataFrame, DataFrame) => DataFrame]

    // Ejecutar la expresión con el DataFrame
    expression(df_1, df_2)
  }
}
