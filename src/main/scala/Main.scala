import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

import java.io.PrintWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Main {
    // TODO: aggiungere data structure per serie ottenuta da log
    var timeLog = Vector.empty[Long]

    def time[R](block: => R): (R, Long) = {
        val t0 = System.currentTimeMillis()
        val result = block
        val t1 = System.currentTimeMillis()
        (result, t1 - t0)
    }

    def dump(filePath: String) {
        val conf = new Configuration()
        val fs: FileSystem = FileSystem.get(conf)
        val file = fs.create(new Path(filePath))
        val bw = new PrintWriter(file)
        
        val csvLog = "iteration time\n" ++ (timeLog map(_.toString) mkString("\n")
)

        bw.write(csvLog)
        bw.close()
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
//            case "SPRING" =>    log[SparkGraph, SPRING](SPRING, sc, 5, inFilePath, outFilePath)
            case "FR-M" =>      log[MutableGraph, FruchtermanReingoldMutable.type](FruchtermanReingoldMutable, sc, 5, inFilePath, outFilePath)
            case "FR-S" =>      log[SparkGraph, FruchtermanReingoldSpark.type](FruchtermanReingoldSpark, sc, 5, inFilePath, outFilePath)
            case name => println(s"$name not recognized")
        }
        println("\n")
        println(s"$algorithmToRun has ended! I hope you liked it senpai UmU\n")

        val filename =
            if (inFilePath.contains("/") && inFilePath.lastIndexOf("/") != inFilePath.length - 1) {
                val withoutSlash = inFilePath
                    .substring(inFilePath.lastIndexOf("/") + 1)
                withoutSlash.substring(0, withoutSlash.lastIndexOf("."))
            } else {
                inFilePath
                    .substring(0, inFilePath.lastIndexOf("."))
            }

        dump(s"out/timings-$algorithmToRun-$filename.csv")

        // println(s"Elapsed time: $calcTime ms")

        spark.stop()
    }

    def log[T[Point2] <: Graph[Point2], A <: Layouter[T]](algorithm: A, sc: SparkContext, iterations: Int, inFilePath: String, outFilePath: String) {
        var graph = algorithm.start(sc, inFilePath, iterations)
        for (i <- 0 until iterations) {
            val (_, calcTime) = time {
                graph = algorithm.run(i, graph)
            }
            timeLog = timeLog :+ calcTime
            // aggiungi calcTime alla serie
            println(s"Iteration ${i+1}/$iterations completed ($calcTime ms)")
        }
        algorithm.end(graph, outFilePath)
    }
}