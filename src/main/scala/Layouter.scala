import org.apache.spark.SparkContext

trait Layouter[P, T[P] <: Graph[P]] {
    def start(sc: SparkContext, inFilePath: String, iterations: Int): T[P]
    def run(iteration: Int, graph: T[P]): T[P]
    def end(graph: T[P], outFilePath: String)
}