import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object P2Query4 {

  def main(args: Array[String]): Unit = {
    // 1. Create SparkSession
    val spark = SparkSession.builder()
      .appName("Query4DataFrame")
      .master("local[*]")
      .getOrCreate()

    // 2. Read T1 (filtered Purchases with TransTotal <= 100)
    //    Replace "path/to/T1.csv" with your actual file path or directory
    val t1DF = spark.read
      .option("header", "true")       // if your CSV has a header row
      .option("inferSchema", "true")  // to automatically infer column types
      .csv("Purchases.csv")

    // 3. Read Customers
    //    Replace "path/to/Customers.csv" with your actual file path
    val customersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("Customers.csv")

    // 4. Join T1 and Customers on CustID
    //    This will add the columns from Customers onto each matching row in T1
    val joinedDF = t1DF
      .join(customersDF, "CustID")  // "CustID" must exist in both DataFrames

    // 5. Group by each customer (and keep Salary, Address, etc.)
    //    Compute total expenses as the SUM of TransTotal
    val aggregatedDF = joinedDF
      .groupBy("CustID", "Salary", "Address")
      .agg(
        sum("TransTotal").alias("totalExpense")
      )

    // 6. Return only those customers who CANNOT cover their expenses
    //    i.e., Salary < totalExpense
    val finalDF = aggregatedDF
      .filter(col("totalExpense") > col("Salary"))

    // 7. Show the result (or write to a file)
    finalDF.show(truncate = false)

    // If you want to save it as CSV, you can do:
    // finalDF.write.mode("overwrite").csv("path/to/outputQuery4")

    // 8. Stop Spark
    spark.stop()
  }
}