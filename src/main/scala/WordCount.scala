import org.apache.spark.sql.SparkSession

object WordCount {
  def run(args: Array[String]) {
    // Args parsing
    val inPath = args(0)
    val outPath = args(1)

    // Spark initialization
    val spark = SparkSession
      .builder
      .appName("Word Count")
      .config("spark.master", "local")
      .getOrCreate()
    val sc = spark.sparkContext

    val textFile = sc.textFile(inPath)
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(outPath)

    spark.stop()
  }
}