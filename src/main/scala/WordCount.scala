import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Word Count")
      .config("spark.master", "local[2]")
      .getOrCreate()
    val sc = spark.sparkContext
    val textFile = sc.textFile("data/words.txt")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile("out/wordcount")
    spark.stop()
  }
}