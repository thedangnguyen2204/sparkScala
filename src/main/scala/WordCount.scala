import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
object WordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

//    val sc = new SparkContext("local[*]", "WordCount")
//
//    val rdd = sc.textFile("D:\\Viettel\\sparkScala\\book.txt")
//
//    val words = rdd.flatMap(x => x.split("\\W+"))
//
//    val lowerCase = words.map(x => x.toLowerCase())
//
//    val wordCount = lowerCase.map(x => (x, 1)).reduceByKey((x, y) => x + y)
//
//    val wordCountSorted = wordCount.map(x => (x._2, x._1)).sortByKey()
    val sc = new SparkContext("local[*]", "WordCountBetterSorted")

    // Load each line of my book into an RDD
    val input = sc.textFile("D:\\Viettel\\sparkScala\\book.txt")

    // Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))

    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())

    // Count of the occurrences of each word
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey( (x,y) => x + y )

    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey()
    for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
  }
}
