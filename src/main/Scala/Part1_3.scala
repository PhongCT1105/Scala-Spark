import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// Define the case class outside of the Part1 object
case class Person(id: String, name: String, table: String, test: String)

object Part1_3 {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Part1_3")
    val sc = new SparkContext(sparConf)

    val megaEvent = sc.textFile("Mega_Event.csv")

    // Properly parse CSV lines while handling potential commas within quotes
    val peopleRDD = megaEvent.map(line => {
      val parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1) // Handles commas inside quotes
      Person(parts(0).trim, parts(1).trim, parts(2).trim, parts(3).trim)
    })

    val sickPeople = peopleRDD.filter(_.test.toLowerCase == "sick").map(p => (p.table, 1)).distinct()

    val healthyPeople = peopleRDD.filter(_.test.toLowerCase != "sick").map(p => (p.table, p)).distinct()

    val healthyAtRisk = healthyPeople.join(sickPeople).map { case (_, (person, _)) => person }

    healthyAtRisk.collect().foreach(println)
    sc.stop()
  }
}