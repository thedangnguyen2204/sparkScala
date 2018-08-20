import org.apache.spark.SparkContext
import org.apache.log4j._
import org.apache.spark.graphx
import scala.math.min
object MinTemperatures {
  def parseLines(line: String) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.9f) + 32.0f
    (stationID, entryType, temperature)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MinTemperatures")

    val lines = sc.textFile("D:\\Viettel\\sparkScala\\1800.csv")

    val reducedData = lines.map(parseLines)

    val filter = reducedData.filter(x => x._2 == "TMIN")

    val minTem = filter.map(x => (x._1, x._3.toFloat))

    val minTemByStation = minTem.reduceByKey( (x, y) => min(x, y) )

    val results = minTemByStation.collect()

    results.foreach(println)

  }
}
