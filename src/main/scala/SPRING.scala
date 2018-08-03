/* 
    Implementation of 1984 algorithm by Eades 
    Eades, Peter. A heuristic for graph drawing Stanton. Ralph G., editor. Congressus numerantium. Winnipeg: Utilitas Mathematica; 1984 May: 149-160. ISBN: 0-919628-42-7.
*/

import org.apache.spark._
import org.apache.spark.graphx.{Graph => XGraph, Edge, VertexId}
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.rdd.RDD

object SPRING extends Layouter {
    // original uses c1 = 2, c2 = 1, c3 = 1, c4 = 0.1, and M = 100
    val (c1, c2, c3, c4) = (4.0, 1.0, 2.0, 0.01)
    val (width, length) = (1000, 1000)

    // Algorithm forces
    def attractiveForce(d: Double) = c1 * math.log((d + 0.0001) / c2)
    def repulsiveForce(d: Double) = c3 / (math.sqrt(d) + 0.0001)

    // Parallel version
    def runSpark(sc: SparkContext, iterations: Int, inFilePath: String, outFilePath: String) {
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
                    List((t.srcId, -displacement), (t.dstId, displacement))
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
        val normVertices = (vertices map (v => new Point2(v._2.x/maxX * width, v._2.y/maxY * length)) collect).toList
        Pajek.dump(new ImmutableGraph(normVertices, parsedGraph.edges), outFilePath)
    }

    def start(sc: SparkContext, inFilePath: String, iterations: Int): Graph[Point2] = {
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

        new SparkGraph[Point2](initialGraph)
    }

    def run(iteration: Int, g: Graph[Point2]): Graph[Point2] = {

        val graph: XGraph[Point2, Null] = g match {
            case SparkGraph(graph: XGraph[Point2, Null]) => graph
            case _ => throw new IllegalArgumentException
        }

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
                List((t.srcId, -displacement), (t.dstId, displacement))
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

        //modifiedGraph.vertices.foreach(println)
        modifiedGraph.checkpoint
        new SparkGraph[Point2](modifiedGraph)
    }

    def end(graph: Graph[Point2], outFilePath: String) {
        val maxX = ((graph.vertices.collect map {
            case (id, pos) => pos.x
        }) max)
        val maxY = ((graph.vertices.collect map {
            case (id, pos) => pos.y
        }) max)
        println(maxX)
        println(maxY)
        //val normVertices = (vertices map (v => new Point2(v._2.x/maxX * width, v._2.y/maxY * length)) collect).toList
        Pajek.dump(ImmutableGraph.fromSpark(graph mapVertices {
            case (id, pos) => new Point2(pos.x/maxX * width, pos.y/maxY * length)
        }), outFilePath)
    }

    def niam(args: Array[String]) {

        val maxIter = 1000
        val graph = Pajek.parse(args(0))
        val vertexNum = graph.vertices.size
        
        val edges = graph.edges
        
        import scala.collection.mutable.ListBuffer
        var vertices: Array[(Double, Double)] = new Array(vertexNum)

        for (i <- 0 until vertexNum) {
            vertices(i) = ((Math.random, Math.random))
        }

        for (i <- 0 to maxIter) {
            val t0 = System.currentTimeMillis()
            // Repulsive forces iteration
            for (
                v <- 0 until (vertexNum - 1);
                u <- (v + 1) until vertexNum
            ) {
                // repulsiveForce force
                // if you have a vector (v - u) (i.e. u -> v) then the repulsiveForce force adds to v and substracts to u ???
                val distance = ((vertices(v)._1 - vertices(u)._1), (vertices(v)._2 - vertices(u)._2))
                val length = Math.sqrt(Math.pow(distance._1, 2) + Math.pow(distance._2, 2))
                val normDistance = (distance._1 / length, distance._2 / length)

                val repulsive = repulsiveForce(length) * c4

                val displacement = (normDistance._1 * repulsive, normDistance._2 * repulsive)
                vertices(v) = (vertices(v)._1 + displacement._1, vertices(v)._2 + displacement._2)
                vertices(u) = (vertices(u)._1 - displacement._1, vertices(u)._2 - displacement._2)
            }

            // Attractive forces iteration
            for ((v, u) <- edges) {
                val distance = ((vertices(v - 1)._1 - vertices(u - 1)._1), (vertices(v - 1)._2 - vertices(u - 1)._2))
                val length = Math.sqrt(Math.pow(distance._1, 2) + Math.pow(distance._2, 2))
                val normDistance = (distance._1 / length, distance._2 / length)

                val attractive = attractiveForce(length) * c4

                val displacement = (normDistance._1 * attractive, normDistance._2 * attractive)
                vertices(v - 1) = (vertices(v - 1)._1 - displacement._1, vertices(v - 1)._2 - displacement._2)
                vertices(u - 1) = (vertices(u - 1)._1 + displacement._1, vertices(u - 1)._2 + displacement._2)
            }
    
            val t1 = System.currentTimeMillis()
            println(s"iteration $i took ${t1 - t0}ms")
        }
        
        val layoutedVertices = (0 until vertexNum) map {
            case i => new Point2(vertices(i)._1, vertices(i)._2)
        } toList

        val (maxX, maxY) = ((layoutedVertices map(_.x)) max, (layoutedVertices map(_.y)) max)
        val layoutedNorm = layoutedVertices map (p => new Point2(p.x/maxX * width, p.y/maxY * length))
        Pajek.dump(new ImmutableGraph(layoutedNorm, graph.edges), args(1))
    }
}