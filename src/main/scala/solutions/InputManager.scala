/** Created By O002292
 * Date: 15/03/2024
 * Time: 12:13
 */

package solutions

import java.io.FileWriter
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.util.Try

case class Properties(path: String, schema: String, data_type: String, filter: String)

object InputManager extends App {

  private val inputs_path = "src/main/resources/inputs.json"

  def loadInputs(): Map[String, Properties] = {
    try {
      val json_string = scala.io.Source.fromFile(inputs_path).mkString
      implicit val formats: DefaultFormats.type = DefaultFormats
      parse(json_string).extract[Map[String, Properties]]
    } catch {
      case e: Exception =>
        Map.empty[String, Properties]
    }
  }

  def loadInput(input_key: String): Properties = {
    val input = Try(loadInputs()(input_key)).toOption

    input match {
      case Some(properties) => properties
      case None => Properties("", "", "", "")
    }
  }

  def addInput(new_input_key: String, new_input_value: Properties): Unit = {

    val inputs = loadInputs + (new_input_key -> new_input_value)

    implicit val formats: DefaultFormats.type = DefaultFormats
    val json = Extraction.decompose(inputs)

    val writer = new FileWriter(inputs_path)
    try {
      writer.write(pretty(render(json)))
    } finally {
      writer.close()
    }

    println("Archivo JSON generado exitosamente.")
  }

  // Ejemplo de uso: cargar el archivo JSON en un Map[String, Attributes]
  addInput("table 1", Properties("path1", "schema1", "type1", "filter1"))
  val inputs = loadInputs()

  println(loadInput("atributo4").toString)
}
