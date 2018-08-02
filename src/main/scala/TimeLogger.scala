import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph => XGraph, Edge, VertexId}

trait LogMeBabyOneMoreTime {
    def start(sc: SparkContext, inFilePath: String): XGraph[Point2, Null]
    def run(interation: Int, graph: XGraph[Point2, Null]): XGraph[Point2, Null]
    def end(graph: XGraph[Point2, Null], outFilePath: String)
}

object TimeLogger {
    /*
    def log(algorithm: LogMeBabyOneMoreTime) = {
        algorithm.start()
        algorithm.run()
        algorithm.end()
    }
    */
}