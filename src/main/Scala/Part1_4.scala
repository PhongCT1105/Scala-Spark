import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// Define a case class for structured data
//case class Person(id: String, name: String, table: String, test: String)

object Part1Query4 {

  def main(args: Array[String]): Unit = {
    // Initialize Spark
    val sparkConf = new SparkConf().setMaster("local").setAppName("Query4")
    val sc = new SparkContext(sparkConf)

    // Read the dataset
    val megaEvent: RDD[String] = sc.textFile("Mega_Event.csv")

    // Parse the CSV into a structured RDD
    val peopleRDD: RDD[Person] = megaEvent.map(line => {
      val parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1) // Handles commas inside quotes
      Person(parts(0).trim, parts(1).trim, parts(2).trim, parts(3).trim)
    })

    // Compute the number of people at each table
    val tableCounts: RDD[(String, Int)] = peopleRDD.map(person => (person.table, 1)).reduceByKey(_ + _)

    // Check if each table contains only healthy people
    val tableHealthStatus: RDD[(String, String)] = peopleRDD
      .map(person => (person.table, person.test.toLowerCase)) // Extract (table, test result)
      .groupByKey() // Group all test results by table
      .mapValues(tests => if (tests.forall(_ == "healthy")) "healthy" else "concern") // Determine flag

    // Join the count and health status
    val result: RDD[(String, Int, String)] = tableCounts.join(tableHealthStatus).map {
      case (table, (count, status)) => (table, count, status)
    }

    // Print the result
    result.collect().foreach { case (table, count, status) =>
      println(s"Table: $table, People: $count, Flag: $status")
    }

    // Stop Spark
    sc.stop()
  }
}
