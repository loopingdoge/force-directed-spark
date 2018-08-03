import org.apache.spark.SparkContext

trait Layouter {
    def start(sc: SparkContext, inFilePath: String, iterations: Int): Graph[Point2]
    def run(iteration: Int, graph: Graph[Point2]): Graph[Point2]
    def end(graph: Graph[Point2], outFilePath: String)
}
