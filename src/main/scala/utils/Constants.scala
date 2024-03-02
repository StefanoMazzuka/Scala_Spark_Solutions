/** Created By O002292
 * Date: 20/11/2023
 * Time: 12:16
 */

package utils

object Constants {
  val TEST_DATA_PATH  = "src/test/resources/data/"
  val CSV_EXTENSION   = ".csv"
  val LOCAL_HDFS_PATH = System.getenv("LOCAL_HDFS_PATH")
}

object Options {
  val DELIMITER = "delimiter"
  val HEADER    = "header"
}

object Characters {
  val TABLE_SPLIT    = "_"
  val PATH_SPLIT     = "/"
  val DELIMITER_TYPE = "|"
  val WHITE_SPACE    = " "
  val EMPTY_STRING   = ""
}

object JoinTypes {
  val INNER = "inner"
  val LEFT  = "left"
  val RIGHT = "right"
}

object Columns {
  val FIRST_NOT_NULL_COL = "first_not_null_col"
  val COL_WITH_VALUE     = "col_with_value"
  val COL_RESULT         = "col_result"
}