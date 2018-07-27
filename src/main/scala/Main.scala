import org.apache.spark.sql.SparkSession

object Main {

    def time[R](block: => R): Long = {
        val t0 = System.currentTimeMillis()
        val result = block
        val t1 = System.currentTimeMillis()
        t1 - t0
    }

    def main(args: Array[String]) {
        // Spark initialization
        val spark = SparkSession
            .builder
            .appName("Force Directed Layout")
            .config("spark.master", "local[2]")
            .getOrCreate()

        val sc = spark.sparkContext
        sc.setLogLevel("ERROR")

        val calcTime = time {
            FruchtermanReingold.runSpark(
                sc,
                3,
                "data/eth-6h.net",
                "out/eth-6h.net"
            )
        }

        println(s"Elapsed time: $calcTime ms")

        spark.stop()
    }
}