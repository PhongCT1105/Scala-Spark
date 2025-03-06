import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler

object P2Bonus {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark MLlib - Predicting TransTotal")
      .master("local[*]") // Running locally
      .getOrCreate()

    import spark.implicits._

    // Load Customer CSV
    val customersDF: DataFrame = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv("small-data/Customers.csv")

    // Load Purchases CSV
    val purchasesDF: DataFrame = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv("small-data/Purchases.csv")

    // Perform Inner Join on CustomerID
    val joinedDF = customersDF.join(purchasesDF, "CustID")

    // Assemble feature columns into a single vector
    val featureCols = Array("Age", "Salary", "TransNumItems")
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val transformedDF = assembler.transform(joinedDF)

    // Rename the target column to "label"
    val finalDF = transformedDF.select($"features", $"TransTotal".alias("label"))

    // Split dataset into training (80%) and testing (20%)
    val Array(trainDF, testDF) = finalDF.randomSplit(Array(0.8, 0.2), seed = 42)

    // Define Linear Regression model
    val lr = new LinearRegression()
      .setMaxIter(20)
      .setRegParam(0.3)  // Regularization parameter
      .setElasticNetParam(0.8) // Elastic Net (0 = Ridge, 1 = Lasso)

    // Train the model
    val model = lr.fit(trainDF)

    // Print model coefficients and intercept
    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

    // Make predictions
    val predictions = model.transform(testDF)

    // Show predictions
    predictions.select("features", "label", "prediction").show()

    // Stop Spark session
    spark.stop()
  }
}
