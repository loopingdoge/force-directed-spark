/* 
    Implementation of 1984 algorithm by Eades 
    Eades, Peter. A heuristic for graph drawing Stanton. Ralph G., editor. Congressus numerantium. Winnipeg: Utilitas Mathematica; 1984 May: 149-160. ISBN: 0-919628-42-7.
*/

import org.apache.spark._
import org.apache.spark.graphx.{Graph => XGraph, Edge, VertexId}
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.rdd.RDD

object SpringSpark extends Layouter[SparkGraph] {
    val c4 = SpringUtils.c4
    val (width, length) = (SpringUtils.width, SpringUtils.length)
    var nodePairs: RDD[((VertexId, Point2), (VertexId, Point2))] = _

    // Old parallel version
    /* def runSpark(sc: SparkContext, iterations: Int, inFilePath: String, outFilePath: String) {
        // Place vertices at random
        val parsedGraph = Pajek.parse(inFilePath) map { _ => new Point2() }

        // Create the spark graph
        val initialGraph = XGraph(
            sc.parallelize(
                parsedGraph.vertices
                    .zipWithIndex
                    .map { case (v, i) => (i.toLong + 1, v) }
            ),
            sc.parallelize(
                parsedGraph.edges
                    .map { case (u, v) => Edge(u, v, null) }
            )
        )

        // Main cycle
        val computedGraph = (0 until iterations).foldLeft(initialGraph) { (graph, i) =>
            val t0 = System.currentTimeMillis()
            val repulsionDisplacements: RDD[(VertexId, Vec2)] = graph.vertices
                .cartesian(graph.vertices)
                // Remove the pairs having the same ID
                .filter {
                    case ((id1, _), (id2, _)) if id1 == id2 => false
                    case _ => true
                }
                // Calculate the displacement for every pair
                .map {
                    case ((id1, pos1), (id2, pos2)) =>
                        val delta = pos1 - pos2
                        val displacement = delta.normalize * repulsiveForce(delta.length)
                        (id1, displacement)
                }
                // Sum the displacements of the pairs having the same key
                // We finally obtain the displacement for every node
                .reduceByKey((a: Vec2, b: Vec2) => a + b)

            val attractiveDisplacements: RDD[(VertexId, Vec2)] = graph.triplets
                // Calculate the displacement for every edge
                // Flatten the list containing 2 pairs (one for each node of the edge)
                .flatMap { t =>
                    val delta = t.srcAttr - t.dstAttr
                    val displacement = delta.normalize * attractiveForce(delta.length)
                    Vector((t.srcId, -displacement), (t.dstId, displacement))
                }
                .reduceByKey(_ + _)

            // Sum the repulsion and attractive displacements
            val sumDisplacements = repulsionDisplacements
                .union(attractiveDisplacements)
                .reduceByKey(_ + _)
                .collect
                // Transform to a Map in order to get the displacement given the nodeID
                .toMap

            // Obtain a new graph by summing the displacements to the node current position
            val modifiedGraph = graph.mapVertices {
                case (id, pos) =>
                    val vDispl = sumDisplacements(id)
                    val newPos = pos + vDispl.normalize * vDispl.length * c4
                    newPos
            }

            val t1 = System.currentTimeMillis()
            println(s"Iteration ${i + 1}/$iterations, ${t1 - t0} ms")
            //modifiedGraph.vertices.foreach(println)
            modifiedGraph.checkpoint
            modifiedGraph
        }

        val vertices = computedGraph.vertices
        val (maxX, maxY) = ((vertices map(_._2.x)) max, (vertices map(_._2.y)) max)
        val normVertices = (vertices map (v => new Point2(v._2.x/maxX * width, v._2.y/maxY * length)) collect).toVector
        Pajek.dump(new ImmutableGraph(normVertices, parsedGraph.edges), outFilePath)
    } */

    def start(sc: SparkContext, inFilePath: String, iterations: Int): SparkGraph[Point2] = {
        val parsedGraph = Parser.parse(inFilePath) map { _ => new Point2() }

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
        )
        nodePairs = initialGraph.vertices.cartesian(initialGraph.vertices).filter {
            case ((id1, _), (id2, _)) => id1 != id2
        }.cache

        new SparkGraph[Point2](initialGraph)
    }

    def run(iteration: Int, g: SparkGraph[Point2]): SparkGraph[Point2] = {
        val graph: XGraph[Point2, Null] = g.graph

        val repulsionDisplacements: RDD[(VertexId, Vec2)] = this.nodePairs
        .map {
            case ((id1, pos1), (id2, pos2)) =>
                val delta = pos1 - pos2
                val displacement = delta.normalize * SpringUtils.repulsiveForce(delta.length) * c4
                (id1, displacement)
        }
        .reduceByKey((a: Vec2, b: Vec2) => a + b)

        val attractiveDisplacements: RDD[(VertexId, Vec2)] = graph.triplets
            .flatMap { t =>
                val delta = t.srcAttr - t.dstAttr
                val displacement = delta.normalize * SpringUtils.attractiveForce(delta.length) * c4
                Vector((t.srcId, -displacement), (t.dstId, displacement))
            }
            .reduceByKey(_ + _)

        val sumDisplacements = repulsionDisplacements
            .union(attractiveDisplacements)
            .reduceByKey(_ + _)
            .collectAsMap

        val modifiedGraph = graph.mapVertices {
            case (id, pos) =>
                val vDispl = sumDisplacements(id)
                val newPos = pos + vDispl
                newPos
        }

        new SparkGraph[Point2](modifiedGraph)
    }

    def end(g: SparkGraph[Point2], outFilePath: String) = {
        val xgraph: XGraph[Point2, Null] = g.graph

        val maxX = (xgraph.vertices.collect map {
            case (_, pos) => pos.x
        }) max
        val maxY = (xgraph.vertices.collect map {
            case (_, pos) => pos.y
        }) max

        Pajek.dump(ImmutableGraph.fromSpark(xgraph mapVertices {
            case (_, pos) => new Point2(pos.x/maxX * width, pos.y/maxY * length)
        }), outFilePath)
    }
}