import org.apache.spark.SparkContext

object FriendsByAge {
  def parseLine(line: String) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age, numFriends)
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "FriendsByAge")

    val lines = sc.textFile("D:\\Viettel\\sparkScala\\fakefriends.csv")

    val rdd = lines.map(parseLine)

    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    val avgByAge = totalsByAge.mapValues(x => x._1 / x._2)

    val results = avgByAge.collect()

    results.sorted.foreach(println)
  }
}
