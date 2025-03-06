import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}

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

    val outputPath = "output/T1_temp.csv"

    filteredDF.write
      .option("header", "true") // Include header
      .mode("overwrite") // Overwrite if the file exists
      .csv(outputPath)

    // Rename output for future tasks
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val file = fs.globStatus(new Path(s"$outputPath/part*"))(0).getPath.getName
    fs.rename(new Path(s"$outputPath/$file"), new Path("output/T1.csv"))

    // Remove the temporary folder
    fs.delete(new Path(outputPath), true)

    // Stop the SparkSession
    spark.stop()
  }
}