import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object P1Query2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Query2")
    val sc = new SparkContext(conf)

    // 1. Read the files
    val noDisclosureRDD: RDD[String] = sc.textFile("Mega-Event-No-Disclosure.csv")
    val reportedIllnessesRDD: RDD[String] = sc.textFile("Reported-Illnesses.csv")

    // (Optional) If you have headers, remove them:
    // val header1 = noDisclosureRDD.first()
    // val header2 = reportedIllnessesRDD.first()
    // val noDisclosureData = noDisclosureRDD.filter(_ != header1)
    // val reportedIllnessesData = reportedIllnessesRDD.filter(_ != header2)

    // 2. Parse each file into (id, ...) pairs
    // Assume Mega-Event-No-Disclosure columns: id, name, table
    val noDisclosurePairs: RDD[(String, (String, String))] =
    noDisclosureRDD.map { line =>
      val fields = line.split(",")
      val id    = fields(0).trim
      val name  = fields(1).trim
      val table = fields(2).trim
      (id, (name, table))
    }

    // Assume Reported-Illnesses columns: id, test
    // e.g., 123,sick
    val reportedPairs: RDD[(String, String)] =
    reportedIllnessesRDD.map { line =>
      val fields = line.split(",")
      val id   = fields(0).trim
      val test = fields(1).trim
      (id, test)
    }

    // 3. Join both RDDs on id
    // joined => (id, ((name, table), test))
    val joined: RDD[(String, ((String, String), String))] =
    noDisclosurePairs.join(reportedPairs)

    // 4. Filter or simply collect all joined records (since Reported-Illnesses are all "sick")
    // Extract (id, table) for the final output
    val sickAttendees: RDD[(String, String)] =
    joined.map {
      case (id, ((name, table), test)) =>
        // We only need (id, table)
        (id, table)
    }

    // 5. Collect and show results
    sickAttendees.collect().foreach { case (id, table) =>
      println(s"ID: $id, Table: $table (Sick)")
    }

    sc.stop()
  }
}