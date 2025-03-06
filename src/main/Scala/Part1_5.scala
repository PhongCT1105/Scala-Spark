import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// Define the case class outside of the Part1 object
//case class Person(id: String, name: String, table: String, test: String)
case class Sick(id: String, test: String)
case class Participant(id: String, name: String, table: String)

object Part1_5 {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Part1_5")
    val sc = new SparkContext(sparConf)

    val megaEvent = sc.textFile("small-data/Mega_Event_No_Disclosure.csv")
    val sickReport = sc.textFile("small-data/Reported_Illnesses.csv")

    // Properly parse CSV lines while handling potential commas within quotes
    val peopleRDD = megaEvent.map(line => {
      val parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1) // Handles commas inside quotes
      Participant(parts(0).trim, parts(1).trim, parts(2).trim)
    })

    val sickRDD = sickReport.map(line => {
      val parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1) // Handles commas inside quotes
      Sick(parts(0).trim, parts(1).trim)
    })

    val peoplePairRDD = peopleRDD.map(participant => (participant.id, participant))
    val sickPairRDD = sickRDD.map(sick => (sick.id, sick))

    val joinedRDD = peoplePairRDD.leftOuterJoin(sickPairRDD)

    val peopleInfo = joinedRDD.map { case (id, (person, sick)) =>
      val test = sick.map(_.test).getOrElse("not-sick")
      Person(id, person.name, person.table, test)
    }

    val sickPeople = peopleInfo.filter(_.test.toLowerCase == "sick").map(p => (p.table, 1)).distinct()

    val healthyPeople = peopleInfo.filter(_.test.toLowerCase != "sick").map(p => (p.table, p)).distinct()

    val healthyAtRisk = healthyPeople.join(sickPeople).map { case (_, (person, _)) => person }

    healthyAtRisk.collect().foreach(println)
    sc.stop()
  }
}