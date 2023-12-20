/** Created By O002292
 * Date: 20/11/2023
 * Time: 13:24
 */

package Solutions

import Utils.JoinTypes._
import org.apache.spark.sql.DataFrame

object Joins {
  def innerJoin(df_1: DataFrame, df_2: DataFrame, join_columns: List[String]): DataFrame = {
    df_1.join(df_2, join_columns, INNER)
  }

  def leftJoin(df_1: DataFrame, df_2: DataFrame, join_columns: List[String]): DataFrame = {
    df_1.join(df_2, join_columns, LEFT)
  }

  def rightJoin(df_1: DataFrame, df_2: DataFrame, join_columns: List[String]): DataFrame = {
    df_1.join(df_2, join_columns, RIGHT)
  }
}
