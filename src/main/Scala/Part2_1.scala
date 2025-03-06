import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

case class Purchases(TransID: String, CustID: String, TransTotal: Double, TransNumItems: Integer, TransDesc: String)

object Part2_1 {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder()
      .master("local")
      .appName("Part2_1")
      .getOrCreate()

    import spark.implicits._

    // Read CSV files into DataFrames
    val customerDF = spark.read
      .option("header", "true") // If CSV files have headers
      .option("inferSchema", "true") // Infer schema automatically
      .csv("Purchases.csv")
      .as[Purchases] // Convert to Dataset[Purchases]

    val filteredDF = customerDF
      .filter(col("TransTotal").<=(100))

    filteredDF.show()

    filteredDF.write
      .option("header", "true") // Include header
      .mode("overwrite") // Overwrite if the file exists
      .csv("output/task1.csv")

    // Stop the SparkSession
    spark.stop()
  }
}