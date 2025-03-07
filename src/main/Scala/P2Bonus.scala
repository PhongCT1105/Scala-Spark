import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.ml.regression.{DecisionTreeRegressor, DecisionTreeRegressionModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.RegressionEvaluator


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

    val joinedDF = customersDF.join(purchasesDF, "CustID")
    val dataset = joinedDF.select("TransID", "Age", "Salary", "TransNumItems", "TransTotal")

    // Assemble feature columns into a single vector
    val featureCols = Array("Age", "Salary", "TransNumItems")
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val transformedDF = assembler.transform(dataset)

    // Rename the target column to "label"
    val finalDF = transformedDF.select($"features", $"TransTotal".alias("label"))
    val Array(trainset, testset) = finalDF.randomSplit(Array(0.8, 0.2), seed = 42)

    // Define Decision Tree Regression model
    val dt = new DecisionTreeRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxDepth(5)  // Control tree depth to prevent overfitting

    // Train model
    val model: DecisionTreeRegressionModel = dt.fit(trainset)
    val predictions = model.transform(testset)
    predictions.select("features", "label", "prediction").show()

    // Evaluate Model Performance
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")

    val rmse = evaluator.setMetricName("rmse").evaluate(predictions)
    val r2 = evaluator.setMetricName("r2").evaluate(predictions)
    val mae = evaluator.setMetricName("mae").evaluate(predictions)

    println(s"Root Mean Squared Error (RMSE): $rmse")
    println(s"R² Score: $r2")
    println(s"Mean Absolute Error (MAE): $mae")

    // Print the Decision Tree model structure
//    println(s"Decision Tree Model:\n ${model.toDebugString}")

    // Stop Spark session
    spark.stop()
  }
}
