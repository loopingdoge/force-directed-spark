import org.apache.spark._
import org.apache.spark.graphx.{Graph => XGraph, Edge, VertexId}
import org.apache.spark.rdd.RDD

object FruchtermanReingold {
    val width = 800
    val length = 600
    val initialTemperature: Double = width / 10

    def repulsiveForce(k: Double, x: Double): Double = {
        val divisor = if (x == 0) 0.1 else x
        Math.pow(k, 2) / divisor
    }

    def attractiveForce(k: Double, x: Double): Double = {
        val divisor = if (x == 0) 0.1 else x
        Math.pow(x, 2) / divisor
    }

    // Inverse linear temperature decay
    def temperature(currIter: Int, maxIter: Int): Double = {
        - ((currIter - maxIter).toDouble / maxIter) * initialTemperature
    }

    def runSpark(sc: SparkContext, iterations: Int) {
        // Place vertices at random
        val parsedGraph = Pajek.parse("data/sample_graph.net")
            .map { _ => new Point2(Math.random() * width, Math.random() * length) }

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

        println("Initial graph:")
        graph.vertices.foreach(println)

        val area = width * length
        val k = Math.sqrt(area / graph.numVertices) // Optimal pairwise distance

        // Main cycle
        val computedGraphs = (0 until iterations).map { i =>
            val t = temperature(i, iterations)

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
                        val displacement = delta.normalize * repulsiveForce(k, delta.length)
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
                    val displacement = delta.normalize * attractiveForce(k, delta.length)
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
            // while doing some weird stuff to align them and prevent them to go
            // outside of the frame area
            val modifiedGraph = graph.mapVertices {
                case (id, pos) =>
                    val vDispl = sumDisplacements(id)
                    val newPos = pos + vDispl.normalize * Math.min(t, vDispl.length)
                    val framedPos = new Point2(
                        Math.min(width / 2,  Math.max(-width / 2, newPos.x)),
                        Math.min(length / 2, Math.max(-length / 2, newPos.y))
                    )
                    framedPos
            }

            println(s"\nIteration ${i + 1}:")
            modifiedGraph.vertices.foreach(println)

            modifiedGraph
        }

        Pajek.dump(Graph.fromSpark(computedGraphs.last), "out/pajekkone.net")

    }
}