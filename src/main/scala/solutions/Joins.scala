/** Created By O002292
 * Date: 20/11/2023
 * Time: 13:24
 */

package solutions

import utils.JoinTypes._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Joins {
  def innerJoin(df_1: DataFrame, df_2: DataFrame, join_columns: List[String]): DataFrame = {
    df_1.join(df_2, join_columns, INNER)
  }

  def leftJoin(df_1: DataFrame, df_2: DataFrame, join_columns: List[String]): DataFrame = {
    df_1.join(df_2, join_columns, LEFT).select(df_1.columns.map(df_1(_)):_*)
  }

  def rightJoin(df_1: DataFrame, df_2: DataFrame, join_columns: List[String]): DataFrame = {
    df_1.join(df_2, join_columns, RIGHT)
  }
}
