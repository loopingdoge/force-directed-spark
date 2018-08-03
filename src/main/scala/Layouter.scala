import org.apache.spark.SparkContext

trait Layouter[T[Point2] <: Graph[Point2]] {
    def start(sc: SparkContext, inFilePath: String, iterations: Int): T[Point2]
    def run(iteration: Int, graph: T[Point2]): T[Point2]
    def end(graph: T[Point2], outFilePath: String)
}
