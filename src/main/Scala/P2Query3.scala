import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}

object P2Query3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GenZ Purchase Analysis")
      .master("local[*]") // Running locally
      .getOrCreate()

    import spark.implicits._

    val purchaseDF: DataFrame = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv("output/T1.csv")

    val customerDF: DataFrame = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv("small-data/Customers.csv")

    val joinedDF = purchaseDF.join(customerDF, "CustID")

    // Filter age
    val genZDF = joinedDF.filter($"Age".between(18, 21))

    // Temp output path
    val outputPath = "output/T1_temp.csv"

    val groupedDF = genZDF.groupBy("CustID")
      .agg(
        first("Age").alias("Age"),
        sum("TransNumItems").alias("TotalItems"),
        round(sum("TransTotal"), 2).alias("TotalSpent")
      )

    groupedDF.coalesce(1).write.option("header", "true").csv(outputPath)

    // Rename output for future tasks
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val file = fs.globStatus(new Path(s"$outputPath/part*"))(0).getPath.getName
    fs.rename(new Path(s"$outputPath/$file"), new Path("output/T3.csv"))

    // Remove the temporary folder
    fs.delete(new Path(outputPath), true)

    // Stop Spark session
    spark.stop()
  }
}
