import org.apache.spark.sql.SparkSession

object Main {
    def main(args: Array[String]) {
        // Spark initialization
        val spark = SparkSession
            .builder
            .appName("Word Count")
            .config("spark.master", "local[4]")
            .getOrCreate()

        val sc = spark.sparkContext
        sc.setLogLevel("ERROR")

        FruchtermanReingold.runSpark(sc, 100)

        spark.stop()
    }
}