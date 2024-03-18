/** Created By O002292
 * Date: 18/03/2024
 * Time: 15:52
 */

package solutions.flow

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object Flow {

  private val flow_path = "src/main/resources/flow.json"

  def main(args: Array[String]): Unit = {
    val flow = loadFlow()
    generateFlow(flow("step_4"), flow)
  }

  def loadFlow(): Map[String, Step] = {
    try {
      val json_string = scala.io.Source.fromFile(flow_path).mkString
      implicit val formats: DefaultFormats.type = DefaultFormats
      parse(json_string).extract[Map[String, Step]]
    } catch {
      case e: Exception =>
        Map.empty[String, Step]
    }
  }

  def generateFlow(step: Step, flow: Map[String, Step]): String = {
    if(step.input.startsWith("step_")) generateFlow(flow(step.input), flow) else step.input
    if(step.join.startsWith("step_")) generateFlow(flow(step.join), flow) else step.join
    println(step.function)
    step.function
  }
}
