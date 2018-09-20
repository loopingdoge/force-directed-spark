/* 
    Implementation of 1984 algorithm by Eades 
    Eades, Peter. A heuristic for graph drawing Stanton. Ralph G., editor. Congressus numerantium. Winnipeg: Utilitas Mathematica; 1984 May: 149-160. ISBN: 0-919628-42-7.
*/

import org.apache.spark._
import org.apache.spark.graphx.{Edge, GraphLoader, PartitionStrategy, VertexId, VertexRDD, Graph => XGraph}
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.FileSystem

object SpringSpark extends Layouter[Point2, SparkGraph] {
    val c4 = SpringUtils.c4
    val (width, length) = (SpringUtils.width, SpringUtils.length)
    var nodePairs: RDD[((VertexId, Point2), (VertexId, Point2))] = _

    def start(sc: SparkContext, fs: FileSystem,  inFilePath: String, iterations: Int): SparkGraph[Point2] = {
        val parsedGraph = Parser.parse(fs, inFilePath) map { _ => new Point2() }

        // Create the spark graph
        val initialGraph = XGraph(
            sc.parallelize(
                parsedGraph.vertices
                    .zipWithIndex
                    .map { case (v, i) => (i.toLong, v) }
            ),
            sc.parallelize(
                parsedGraph.edges
                    .map { case (u, v) => Edge(u, v, null) }
            )
        ).partitionBy(PartitionStrategy.EdgePartition2D)

        new SparkGraph[Point2](initialGraph)
    }

    def run(iteration: Int, g: SparkGraph[Point2]): SparkGraph[Point2] = {
        val graph = g.graph
        val nodePairs = graph.vertices.cartesian(graph.vertices)
            .filter {
                case ((id1, _), (id2, _)) => id1 != id2
            }

        val repulsionDisplacements = nodePairs
            .map { v =>
                val ((id1, pos1), (id2, pos2)) = v
                val delta = pos1 - pos2
                val displacement = delta.normalize * SpringUtils.repulsiveForce(delta.length) * c4
                (id1, displacement)
            }
            .reduceByKey(_ + _)

        val attractiveDisplacements = graph.triplets
            .flatMap { t =>
                val delta = t.srcAttr - t.dstAttr
                val displacement = delta.normalize * SpringUtils.attractiveForce(delta.length) * c4
                Vector((t.srcId, -displacement), (t.dstId, displacement))
            }
            .reduceByKey(_ + _)

        val sumDisplacements = repulsionDisplacements
            .union(attractiveDisplacements)
            .reduceByKey(_ + _)
        
        val modifiedGraph = graph.joinVertices(sumDisplacements) {
            case (id, pos, displ) =>
                val newPos = pos + displ
                newPos
        }

        new SparkGraph[Point2](modifiedGraph)
    }

    def end(g: SparkGraph[Point2], fs: FileSystem,  outFilePath: String) = {
        val xgraph: XGraph[Point2, Null] = g.graph

        val maxX = (xgraph.vertices.collect map {
            case (_, pos) => pos.x
        }) max
        val maxY = (xgraph.vertices.collect map {
            case (_, pos) => pos.y
        }) max

        Pajek.dump(ImmutableGraph.fromSpark(xgraph mapVertices {
            case (_, pos) => new Point2(pos.x/maxX * width, pos.y/maxY * length)
        }), fs, outFilePath)
    }
}