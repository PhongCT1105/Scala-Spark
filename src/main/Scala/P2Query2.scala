import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object P2Query2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Purchase Analysis")
      .master("local[*]") // Running locally
      .getOrCreate()

    // Load CSV into DataFrame
    val df: DataFrame = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv("output/T1.csv") // wait to test with Q1 output

    // Group by number of items purchased and compute min, max, and median
    val groupedDF = df.groupBy("TransNumItems")
      .agg(
        min("TransTotal").alias("MinSpent"),
        max("TransTotal").alias("MaxSpent"),
        expr("percentile_approx(TransTotal, 0.5)").alias("MedianSpent")
      )

    // Show results
    groupedDF.show()

    // Stop Spark session
    spark.stop()
  }
}
