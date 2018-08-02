import org.apache.spark.sql.SparkSession

object Main {

    def time[R](block: => R): (R, Long) = {
        val t0 = System.currentTimeMillis()
        val result = block
        val t1 = System.currentTimeMillis()
        (result, t1 - t0)
    }

    def main(args: Array[String]) {
        val algorightmToRun = args(0)
        val inFilePath = args(1)
        val outFilePath = args(2)

        // Spark initialization
        val spark = SparkSession
            .builder
            .appName("Force Directed Layout")
            .config("spark.master", "local[*]")
            .getOrCreate()

        val sc = spark.sparkContext
        sc.setLogLevel("ERROR")
        sc.setCheckpointDir("out/checkpoint/")

        val (_, calcTime) = time {
            algorightmToRun match {
                case "FR" => FruchtermanReingold.runSpark(sc, 500, inFilePath, outFilePath)
                case "SPRING" => 
                    var graph = SPRING.start(sc, inFilePath)
                    for (i <- 0 until 100) {
                        val t0 = System.currentTimeMillis()
                        graph = SPRING.run(i, graph)
                        val t1 = System.currentTimeMillis()
                        println(s"iteration $i took ${t1 - t0}ms")
                    }
                    SPRING.end(graph, outFilePath)

                    //SPRING.runSpark(sc, 500, inFilePath, outFilePath)
            }
        }

        println(s"Elapsed time: $calcTime ms")

        spark.stop()
    }
}