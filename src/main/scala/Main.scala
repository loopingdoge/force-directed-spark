import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object Main {
    // TODO: aggiungere data structure per serie ottenuta da log

    def time[R](block: => R): (R, Long) = {
        val t0 = System.currentTimeMillis()
        val result = block
        val t1 = System.currentTimeMillis()
        (result, t1 - t0)
    }

    def main(args: Array[String]) {
        val algorithmToRun = args(0)
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

        /* val (_, calcTime) = time {
            algorithmToRun match {
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
        } */

        println("\n")
        println(s"$algorithmToRun is firing up! Set, ready, go! OwO\n")
        algorithmToRun match {
            case "SPRING" => log(SPRING, sc, 1, inFilePath, outFilePath)
            case name => println(s"$name not recognized")
        }
        println("\n")
        println(s"$algorithmToRun has ended! I hope you liked it senpai ≧ω≦\n")

        // println(s"Elapsed time: $calcTime ms")

        spark.stop()
    }

    def log[A <: Layouter](algorithm: A, sc: SparkContext, iterations: Int, inFilePath: String, outFilePath: String) {
        var graph = algorithm.start(sc, inFilePath, iterations)
        for (i <- 0 until iterations) {
            val (_, calcTime) = time {
                graph = algorithm.run(i, graph)
            }
            // aggiungi calcTime alla serie
            println(s"Iteration ${i+1}/$iterations completed ($calcTime ms)")
        }
        algorithm.end(graph, outFilePath)
    }
}