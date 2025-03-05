import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Query1 {

  def main(args: Array[String]): Unit = {  

    val sparConf = new SparkConf().setMaster("local").setAppName("Query1")

    val sc = new SparkContext(sparConf)

    val lines: RDD[String] = sc.textFile("small-data/Mega_Event.csv")

    val filteredRows = lines
      .map(_.split(",")) // Split each line into an array
      .filter(fields => fields(2) == "sick")
      .map(fields => fields.mkString(",")) // Convert array back to a CSV-formatting

    filteredRows.collect().foreach(println)

    sc.stop()
  }
}
