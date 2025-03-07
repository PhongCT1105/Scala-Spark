import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object P1Query2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Query2")
    val sc = new SparkContext(conf)

    val noDisclosureRDD: RDD[String] = sc.textFile("Mega-Event-No-Disclosure.csv")
    val reportedIllnessesRDD: RDD[String] = sc.textFile("Reported-Illnesses.csv")

    val noDisclosurePairs: RDD[(String, (String, String))] =
    noDisclosureRDD.map { line =>
      val fields = line.split(",")
      val id    = fields(0).trim
      val name  = fields(1).trim
      val table = fields(2).trim
      (id, (name, table))
    }

    val reportedPairs: RDD[(String, String)] =
    reportedIllnessesRDD.map { line =>
      val fields = line.split(",")
      val id   = fields(0).trim
      val test = fields(1).trim
      (id, test)
    }

    val joined: RDD[(String, ((String, String), String))] =
    noDisclosurePairs.join(reportedPairs)

    val sickAttendees: RDD[(String, String)] =
    joined.map {
      case (id, ((name, table), test)) =>
        (id, table)
    }

    sickAttendees.collect().foreach { case (id, table) =>
      println(s"ID: $id, Table: $table (Sick)")
    }

    sc.stop()
  }
}