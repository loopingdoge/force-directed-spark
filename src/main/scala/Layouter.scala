import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem

trait Layouter[P, T[P] <: Graph[P]] {
    def start(sc: SparkContext, fs: FileSystem, inFilePath: String, iterations: Int): T[P]
    def run(iteration: Int, graph: T[P]): T[P]
    def end(graph: T[P], fs: FileSystem, outFilePath: String)
}