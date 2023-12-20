/** Created By O002292
 * Date: 20/11/2023
 * Time: 12:11
 */

package Utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/** Created By O002292
 * Date: 20/11/2023
 * Time: 12:11
 */

object SparkUtils {

  /**
   * Creating Spark Session
   */
  val sparkSession: SparkSession = SparkSession
    .builder()
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .appName("ScalaSparkSolutions")
    .getOrCreate()
  /**
   * Creating Spark Context
   */
  val sparkContext: SparkContext = sparkSession.sparkContext

  /**
   * Setting Spark configurations
   */
  sparkSession.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
  sparkSession.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
  sparkSession.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY")
  sparkSession.conf.set("spark.sql.decimalOperations.allowPrecisionLoss", false)
  sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")

  /**
   * Stop Spark
   */
  def stopSpark: Unit = {
    sparkSession.stop()
    sparkContext.stop()
  }
}
