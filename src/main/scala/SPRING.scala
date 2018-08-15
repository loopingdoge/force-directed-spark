/* 
    Implementation of 1984 algorithm by Eades 
    Eades, Peter. A heuristic for graph drawing Stanton. Ralph G., editor. Congressus numerantium. Winnipeg: Utilitas Mathematica; 1984 May: 149-160. ISBN: 0-919628-42-7.
*/

import org.apache.spark._
import org.apache.spark.graphx.{Graph => XGraph, Edge, VertexId}
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.rdd.RDD

object SPRINGUtils {
    // original uses c1 = 2, c2 = 1, c3 = 1, c4 = 0.1, and M = 100
    val (c1, c2, c3, c4) = (4.0, 1.0, 2.0, 0.01)
    val (width, length) = (1000, 1000)

    // Algorithm forces
    def attractiveForce(d: Double) = c1 * math.log((d + 0.0001) / c2)
    def repulsiveForce(d: Double) = c3 / (math.sqrt(d) + 0.0001)
}

object SPRINGSpark extends Layouter[SparkGraph] {
    val c4 = SPRINGUtils.c4
    val (width, length) = (SPRINGUtils.width, SPRINGUtils.length)
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
                val displacement = delta.normalize * SPRINGUtils.repulsiveForce(delta.length) * c4
                (id1, displacement)
        }
        .reduceByKey((a: Vec2, b: Vec2) => a + b)

        val attractiveDisplacements: RDD[(VertexId, Vec2)] = graph.triplets
            .flatMap { t =>
                val delta = t.srcAttr - t.dstAttr
                val displacement = delta.normalize * SPRINGUtils.attractiveForce(delta.length) * c4
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

object SPRINGMutable extends Layouter[ImmutableGraph] {
    val c4 = SPRINGUtils.c4
    val (width, length) = (SPRINGUtils.width, SPRINGUtils.length)
    var vertices: Array[(Double, Double)] = new Array(0)

    def start(sc: SparkContext, inFilePath: String, iterations: Int): ImmutableGraph[Point2] = {
        val parsedGraph = Parser.parse(inFilePath)
            .map { _ => new Point2(Math.random, Math.random) }

        val vertexNum = parsedGraph.vertices.size

        vertices = new Array[(Double, Double)](vertexNum)
        for (i <- 0 until vertexNum) {
            vertices(i) = (parsedGraph.vertices(i).x, parsedGraph.vertices(i).y)
        }

        new ImmutableGraph[Point2](parsedGraph.vertices, parsedGraph.edges)
    }

    def run(iteration: Int, graph: ImmutableGraph[Point2]): ImmutableGraph[Point2] = {
        val (vertexNum, edges) = (graph.vertices.size, graph.edges)

        // Repulsive forces iteration
        for (
            v <- 0 until (vertexNum - 1);
            u <- (v + 1) until vertexNum
        ) {
            // repulsiveForce force
            // if you have a vector (v - u) (i.e. u -> v) then the repulsiveForce force adds to v and substracts to u ???
            val distance = (vertices(v)._1 - vertices(u)._1, vertices(v)._2 - vertices(u)._2)
            val length = Math.sqrt(Math.pow(distance._1, 2) + Math.pow(distance._2, 2))
            val normDistance = (distance._1 / length, distance._2 / length)

            val repulsive = SPRINGUtils.repulsiveForce(length) * c4

            val displacement = (normDistance._1 * repulsive, normDistance._2 * repulsive)
            vertices(v) = (vertices(v)._1 + displacement._1, vertices(v)._2 + displacement._2)
            vertices(u) = (vertices(u)._1 - displacement._1, vertices(u)._2 - displacement._2)
        }

        // Attractive forces iteration
        for ((v, u) <- edges) {
            val distance = (vertices(v)._1 - vertices(u)._1, vertices(v)._2 - vertices(u)._2)
            val length = Math.sqrt(Math.pow(distance._1, 2) + Math.pow(distance._2, 2))
            val normDistance = (distance._1 / length, distance._2 / length)

            val attractive = SPRINGUtils.attractiveForce(length) * c4

            val displacement = (normDistance._1 * attractive, normDistance._2 * attractive)
            vertices(v) = (vertices(v)._1 - displacement._1, vertices(v)._2 - displacement._2)
            vertices(u) = (vertices(u)._1 + displacement._1, vertices(u)._2 + displacement._2)
        }

        new ImmutableGraph((0 until vertexNum) map (i => new Point2(vertices(i)._1, vertices(i)._2)) toVector, edges)
    }

    def end(graph: ImmutableGraph[Point2], outFilePath: String) = {
        val (vertexNum, edges) = (graph.vertices.size, graph.edges)

        val layoutedVertices = (0 until vertexNum) map (i => new Point2(vertices(i)._1, vertices(i)._2)) toVector
        val (maxX, maxY) = ((layoutedVertices map(_.x)) max, (layoutedVertices map(_.y)) max)
        val layoutedNorm = layoutedVertices map (p => new Point2(p.x/maxX * width, p.y/maxY * length))
        Pajek.dump(new ImmutableGraph(layoutedNorm, edges), outFilePath)
    }
}