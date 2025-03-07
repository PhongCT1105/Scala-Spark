import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class Person(id: String, name: String, table: String, test: String)

object P1Query4 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("Query4")
    val sc = new SparkContext(sparkConf)

    val megaEvent: RDD[String] = sc.textFile("MegaEvent.csv")

    val peopleRDD: RDD[Person] = megaEvent.map(line => {
      val parts = line.split(",(?=(?:[^"]"[^"]")[^"]$)", -1) // Handles commas inside quotes
      Person(parts(0).trim, parts(1).trim, parts(2).trim, parts(3).trim)
    })

    val tableCounts: RDD[(String, Int)] = peopleRDD.map(person => (person.table, 1)).reduceByKey( + )

    val tableHealthStatus: RDD[(String, String)] = peopleRDD
      .map(person => (person.table, person.test.toLowerCase))
      .groupByKey()
      .mapValues(tests => if (tests.forall( == "healthy")) "healthy" else "concern")

    val result: RDD[(String, Int, String)] = tableCounts.join(tableHealthStatus).map {
      case (table, (count, status)) => (table, count, status)
    }

    result.collect().foreach { case (table, count, status) =>
      println(s"Table: $table, People: $count, Flag: $status")
    }

    sc.stop()
  }
}