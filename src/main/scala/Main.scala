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

    def dump(filePath: String, isCloud: Boolean) {
        val conf = new Configuration()
        val fs: FileSystem =
            if (isCloud)
                new Path("gs://force-directed-bucket").getFileSystem(conf)
            else
                FileSystem.get(conf)
        val file = fs.create(new Path(filePath))
        val bw = new PrintWriter(file)
        
        val csvLog = "iteration time\n" ++ (timeLog map(_.toString) mkString("\n"))

        bw.write(csvLog)
        bw.close()
    }

    def main(args: Array[String]) {

        if( args.length < 2 ) {            
            print("""
    Usage: run algorithm inFile [outFile, isCloud, nCPUs]

        - algorithm | SPRING-M, SPRING-S, FR-M, FR-S, FA2-M
        - inFile | input file name, picked from the "data" folder
        - outFile | optional output file name, saved in the "out" folder
        - isCloud | optional whether or not is executing on GCloud
        - nCPUs | optional CPUs number to use

            """)
            return
        }

        val isCloud = args.length >= 4 && args(3) == "cloud"

        val algorithmToRun = args(0)
        val inFilePath =
            if (isCloud)
                "gs://force-directed-bucket/data/" + args(1)
            else
                "data/" + args(1)
        val outFileName = if( args.length == 2 ) args(1).replace(".txt", ".net") else args(2)
        val outFilePath =
            if (isCloud)
                "gs://force-directed-bucket/out/" + outFileName
            else
                "out/" + outFileName

        val nCPUs =
            if (args.length >= 5)
                args(5)
            else
                "*"


        // Spark initialization
        val spark = SparkSession
            .builder
            .appName("Force Directed Layout")
            .config("spark.master", s"local[$nCPUs]")
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
            case "SPRING-M" =>  log[Point2, ImmutableGraph, SpringMutable.type](SpringMutable, sc, 5, inFilePath, outFilePath)
            case "SPRING-S" =>  log[Point2, SparkGraph, SpringSpark.type](SpringSpark, sc, 5, inFilePath, outFilePath)
            case "FR-M" =>      log[Point2, MutableGraph, FRMutable.type](FRMutable, sc, 5, inFilePath, outFilePath)
            case "FR-S" =>      log[Point2, SparkGraph, FRSpark.type](FRSpark, sc, 5, inFilePath, outFilePath)
            case "FA2-M" =>     log[Point2, MutableGraph, FA2Mutable.type](FA2Mutable, sc, 500, inFilePath, outFilePath)
            case "FA2-S" =>     log[(Point2, Int), SparkGraph, FA2Spark.type](FA2Spark, sc, 500, inFilePath, outFilePath)
            case name => println(s"$name not recognized")
        }
        println("\n")
        println(s"$algorithmToRun has ended! I hope you liked it senpai UmU\n")

        val filename =
            if (inFilePath.contains("/") && inFilePath.lastIndexOf("/") != inFilePath.length) {
                val withoutSlash = inFilePath
                    .substring(inFilePath.lastIndexOf("/") + 1)
                withoutSlash.substring(0, withoutSlash.lastIndexOf("."))
            } else {
                inFilePath
                    .substring(0, inFilePath.lastIndexOf("."))
            }

        dump(s"out/timings-$algorithmToRun-$filename.csv", isCloud)

        // println(s"Elapsed time: $calcTime ms")

        spark.stop()
    }

    def log[P, T[P] <: Graph[P], A <: Layouter[P, T]](algorithm: A, sc: SparkContext, iterations: Int, inFilePath: String, outFilePath: String) {
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