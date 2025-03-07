import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object P2Query4 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Query4DataFrame")
      .master("local[*]")
      .getOrCreate()

    val t1DF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("Purchases.csv")

    val customersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("Customers.csv")

    val joinedDF = t1DF
      .join(customersDF, "CustID")  // "CustID" must exist in both DataFrames

    val aggregatedDF = joinedDF
      .groupBy("CustID", "Salary", "Address")
      .agg(
        sum("TransTotal").alias("totalExpense")
      )

    val finalDF = aggregatedDF
      .filter(col("totalExpense") > col("Salary"))

    finalDF.show(truncate = false)

    spark.stop()
  }
}