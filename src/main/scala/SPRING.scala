/* 
    Implementation of 1984 algorithm by Eades 
    Eades, Peter. A heuristic for graph drawing Stanton. Ralph G., editor. Congressus numerantium. Winnipeg: Utilitas Mathematica; 1984 May: 149-160. ISBN: 0-919628-42-7.
*/

import org.apache.spark._
import org.apache.spark.graphx.{Graph => XGraph, Edge, VertexId}
import org.apache.spark.rdd.RDD

object SPRING {
    // original uses c1 = 2, c2 = 1, c3 = 1, c4 = 0.1, and M = 100
    val (c1, c2, c3, c4) = (2.0, 1.0, 1.0, 0.1)
    val (width, length) = (3000, 3000)

    def attractiveForce(d: Double) = c1 * math.log(d / c2)
    def repulsiveForce(d: Double) = c3 / (math.sqrt(d) + 0.00001)
    def attractiveForce(d: Vec2) = new Point2(math.log(d.x / c2), math.log(d.y / c2)) * c1
    def repulsiveForce(d: Vec2) = new Point2(1.0/(math.sqrt(d.x) + 0.00001), 1.0/(math.sqrt(d.y) + 0.00001)) * c3

    // Sequential but more functional-like version using unmutables
    def run(iteration: Int, allPairs: List[(Int, Int)], edges: List[(Int, Int)], vertices: List[Point2]): List[Point2] = {
        if (iteration == 0) {
            vertices
        } else {
            val pairsDist = allPairs map {
                case (v, u) => 
                    val delta = vertices(v) - vertices(u)
                    (v, u, delta)
            }

            // Compute repulsive forces for every pair independent of the fact that they are linked or not 
            val repulsiveDisplacements = pairsDist map {
                case (v, u, delta) => 
                    val repulsive = delta.normalize * repulsiveForce(delta.length)
                    (v, repulsive)
            }

            val attractiveDisplacements = pairsDist filter {
                // filter pairs that are connected by DIRECTED edges
                case (v, u, d) if edges contains (v, u) => true
                case _ => false
            } flatMap {
                // for every pair compute the attractive displacements
                case (v, u, delta) => 
                    val attractive = delta.normalize * attractiveForce(delta.length)
                    List((v, -attractive), (u, attractive))
            }

            val displacementsSum = (repulsiveDisplacements ++ attractiveDisplacements).groupBy(_._1) map {
                case (key, value) => key -> (value map(_._2) reduce (_ + _))
            }

            val newVertices = displacementsSum map {
                case (key, value) => vertices(key) + value * c4
            } toList

            run(iteration - 1, allPairs, edges, newVertices)
        }
    }

    // Parallel version
    def runSpark(sc: SparkContext, iterations: Int, inFilePath: String, outFilePath: String) {
        // Place vertices at random
        val parsedGraph = Pajek.parse(inFilePath) map { _ => new Point2() }

        // Create the spark graph
        val graph = XGraph(
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
        val computedGraphs = (0 until iterations).map { i =>
            val repulsionDisplacements: RDD[(VertexId, Vec2)] = graph.vertices
                // Generate every possible node pairs
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
                    val newPos = pos + vDispl.normalize * vDispl.length * 0.01
                    newPos
            }

            println(s"Iteration ${i + 1}/$iterations")
            //modifiedGraph.vertices.foreach(println)
            modifiedGraph
        }

        val outGraph = Graph.fromSpark(computedGraphs.last)

        val vertices = outGraph.vertices
        val (maxX, maxY) = ((vertices map(_.x)) max, (vertices map(_.y)) max)
        val normVertices = vertices map (p => new Point2(p.x/maxX * width, p.y/maxY * length))
        Pajek.dump(new Graph(normVertices, outGraph.edges), outFilePath)
    }

    /* def main(args: Array[String]) {
        val maxIter = 100
        val graph = Pajek.parse(args(0))
        val vertexNum = graph.vertices.size
        
        val vertices = (0 until vertexNum) map (_ => new Point2()) toList
        val allPairs = (
            for (
                x <- 0 until vertexNum; 
                y <- 0 until vertexNum if x != y
            ) yield (x, y)
        ) toList

        val layoutedVertices = run(maxIter, allPairs, graph.edges, vertices)
        val (maxX, maxY) = ((layoutedVertices map(_.x)) max, (layoutedVertices map(_.y)) max)
        val layoutedNorm = layoutedVertices map (p => new Point2(p.x/maxX * width, p.y/maxY * length))

        Pajek.dump(new Graph(layoutedNorm, graph.edges), args(1))

        // Sequential and mutable SPRING
        var vertices = (0 to vertexNum) map (_ => new Point2()) toList
        for (_ <- 0 to maxIter) {
            for (
                u <- 0 to vertexNum;
                v <- (u + 1) to vertexNum if (u != v)
            ) {
                val distance = (vertices(v) - vertices(u)).abs()
                if (edges contains (v, u)) {
                    // attractiveForce force between v and u
                    // if you have a vector (v - u) (i.e. u -> v) then the attractiveForce force adds to u and substracts to v ???
                    val attractive = attractiveForce(distance * c4)
                    vertices(v) = vertices(v).shift(-attractive.x, -attractive.y) 
                    vertices(u) = vertices(u).shift(attractive.x, attractive.y)
                } else {
                    // repulsiveForce force
                    // if you have a vector (v - u) (i.e. u -> v) then the repulsiveForce force adds to v and substracts to u ???
                    val repulsive = repulsiveForce(distance * c4)
                    vertices(v) = vertices(v).shift(repulsive.x, repulsive.y)
                    vertices(u) = vertices(u).shift(-repulsive.x, -repulsive.y)
                }
            }
        }
    } */
}