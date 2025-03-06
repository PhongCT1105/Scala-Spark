import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object P2Query3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GenZ Purchase Analysis")
      .master("local[*]") // Running locally
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv("output/T1.csv") // wait for Q1

    // Filter age
    val genZDF = df.filter($"Age".between(18, 21))

    // Group by CustomerID and compute total items and total amount
    val groupedDF = genZDF.groupBy("CustomerID", "Age")
      .agg(
        sum("TransNumItems").alias("TotalItems"),
        sum("TransTotal").alias("TotalSpent")
      )

    // Store result as T3
    groupedDF.write.option("header", "true").csv("output/T3.csv")

    // Stop Spark session
    spark.stop()
  }
}
