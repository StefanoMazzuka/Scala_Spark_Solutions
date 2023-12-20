/** Created By O002292
 * Date: 05/12/2023
 * Time: 11:38
 */

package Solutions

object Collections {

  def main(args: Array[String]): Unit = {
    sortListBasedOnAnother()
  }

  private def sortListBasedOnAnother(): Unit = {
    val A: List[String] = List("a", "d", "f", "s", "e", "h")
    val B: List[String] = List("f", "e", "s")

    val A_map = A.zipWithIndex.toMap
    // Sort sql_columns base in partitions order
    val C     = B.sortBy(e => A_map.get(e))
    // Take last element for sql_columns_sorted
    val last_elemnet = C.last
    val result = A.takeWhile(e => e <= last_elemnet)
    println(result)

    /*
        // Creates a Map[index, partition]
//    val partitions_map     = table_info.partitions.zipWithIndex.toMap
//    // Sort sql_columns base in partitions order
//    val sql_columns_sorted = sql_columns.sortBy(element =>
//      partitions_map.getOrElse(element, Int.MaxValue))
//    // Take last element for sql_columns_sorted
//    val element            = sql_columns_sorted.last
//
//    // Return partitions until element
//    table_info.partitions.reverse.dropWhile(_ != element).reverse
     */
  }
}
