import org.apache.spark._
import org.apache.spark.graphx.{Graph => XGraph, Edge, VertexId}
import org.apache.spark.rdd.RDD

object FruchtermanReingoldUtils {
    val width = 1000
    val length = 1000
    val initialTemperature: Double = width / 10

    def repulsiveForce(k: Double, x: Double): Double = {
        val divisor = if (x == 0) 0.001 else x
        Math.pow(k, 2) / divisor
    }

    def attractiveForce(k: Double, x: Double): Double = {
        val divisor = if (x == 0) 0.001 else x
        Math.pow(x, 2) / divisor
    }

    // Inverse linear temperature decay
    def temperature(currIter: Int, maxIter: Int): Double = {
        -((currIter - maxIter).toDouble / maxIter) * initialTemperature
    }
}

object FruchtermanReingoldMutable extends Layouter {
    private val width = FruchtermanReingoldUtils.width
    private val length = FruchtermanReingoldUtils.length
    private val initialTemperature = FruchtermanReingoldUtils.initialTemperature

    private def repulsiveForce(k: Double, x: Double) = FruchtermanReingoldUtils.repulsiveForce(k, x)
    private def attractiveForce(k: Double, x: Double) = FruchtermanReingoldUtils.attractiveForce(k, x)
    private def temperature(currIter: Int, maxIter: Int) = FruchtermanReingoldUtils.temperature(currIter, maxIter)

    def runSpark(sc: SparkContext, iterations: Int, inFilePath: String, outFilePath: String) {
        // Place vertices at random
        val parsedGraph = Pajek.parse(inFilePath)
            .map { _ => new Point2(Math.random() * width, Math.random() * length) }

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

        val area = width * length
        val k = Math.sqrt(area / initialGraph.numVertices) // Optimal pairwise distance

        // Main cycle
        val computedGraph = (0 until iterations).foldLeft(initialGraph) { case (graph, i) =>
            val t0 = System.currentTimeMillis()
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
//                    This seems to be useless and aesthetically unpleasant...
//                    val framedPos = new Point2(
//                        Math.min(width / 2, Math.max(-width / 2, newPos.x)),
//                        Math.min(length / 2, Math.max(-length / 2, newPos.y))
//                    )
                    newPos
            }

            val t1 = System.currentTimeMillis()
            println(s"Iteration ${i + 1}/$iterations, ${t1 - t0} ms")

            //            modifiedGraph.vertices.foreach(println)
            modifiedGraph.checkpoint
            modifiedGraph
        }

        Pajek.dump(ImmutableGraph.fromSpark(computedGraph), outFilePath)

    }

    override def start(sc: SparkContext, inFilePath: String): Graph[Point2] = {

    }

    override def run(iteration: Int, graph: Graph[Point2]): Graph[Point2] = {

    }

    override def end(graph: Graph[Point2], outFilePath: String): Unit = {

    }
}

object FruchtermanReingoldSpark extends Layouter {
    private val width = FruchtermanReingoldUtils.width
    private val length = FruchtermanReingoldUtils.length
    private val initialTemperature = FruchtermanReingoldUtils.initialTemperature

    private def repulsiveForce(k: Double, x: Double) = FruchtermanReingoldUtils.repulsiveForce(k, x)
    private def attractiveForce(k: Double, x: Double) = FruchtermanReingoldUtils.attractiveForce(k, x)
    private def temperature(currIter: Int, maxIter: Int) = FruchtermanReingoldUtils.temperature(currIter, maxIter)

//        def runSpark(sc: SparkContext, iterations: Int, inFilePath: String, outFilePath: String) {
//            // Place vertices at random
//            val parsedGraph = Pajek.parse(inFilePath)
//                .map { _ => new Point2(Math.random() * width, Math.random() * length) }
//
//            // Create the spark graph
//            val initialGraph = XGraph(
//                sc.parallelize(
//                    parsedGraph.vertices
//                        .zipWithIndex
//                        .map { case (v, i) => (i.toLong + 1, v) }
//                ),
//                sc.parallelize(
//                    parsedGraph.edges
//                        .map { case (u, v) => Edge(u, v, null) }
//                )
//            )
//
//            val area = width * length
//            val k = Math.sqrt(area / initialGraph.numVertices) // Optimal pairwise distance
//
//            // Main cycle
//            val computedGraph = (0 until iterations).foldLeft(initialGraph) { case (graph, i) =>
//                val t0 = System.currentTimeMillis()
//                val t = temperature(i, iterations)
//
//                val repulsionDisplacements: RDD[(VertexId, Vec2)] = graph.vertices
//                    // Generate every possible node pairs
//                    .cartesian(graph.vertices)
//                    // Remove the pairs having the same ID
//                    .filter {
//                    case ((id1, _), (id2, _)) if id1 == id2 => false
//                    case _ => true
//                }
//                    // Calculate the displacement for every pair
//                    .map {
//                    case ((id1, pos1), (id2, pos2)) =>
//                        val delta = pos1 - pos2
//                        val displacement = delta.normalize * repulsiveForce(k, delta.length)
//                        (id1, displacement)
//                }
//                    // Sum the displacements of the pairs having the same key
//                    // We finally obtain the displacement for every node
//                    .reduceByKey((a: Vec2, b: Vec2) => a + b)
//
//                val attractiveDisplacements: RDD[(VertexId, Vec2)] = graph.triplets
//                    // Calculate the displacement for every edge
//                    // Flatten the list containing 2 pairs (one for each node of the edge)
//                    .flatMap { t =>
//                    val delta = t.srcAttr - t.dstAttr
//                    val displacement = delta.normalize * attractiveForce(k, delta.length)
//                    List((t.srcId, -displacement), (t.dstId, displacement))
//                }
//                    .reduceByKey(_ + _)
//
//                // Sum the repulsion and attractive displacements
//                val sumDisplacements = repulsionDisplacements
//                    .union(attractiveDisplacements)
//                    .reduceByKey(_ + _)
//                    .collect
//                    // Transform to a Map in order to get the displacement given the nodeID
//                    .toMap
//
//                // Obtain a new graph by summing the displacements to the node current position
//                // while doing some weird stuff to align them and prevent them to go
//                // outside of the frame area
//                val modifiedGraph = graph.mapVertices {
//                    case (id, pos) =>
//                        val vDispl = sumDisplacements(id)
//                        val newPos = pos + vDispl.normalize * Math.min(t, vDispl.length)
//    //                    This seems to be useless and aesthetically unpleasant...
//    //                    val framedPos = new Point2(
//    //                        Math.min(width / 2, Math.max(-width / 2, newPos.x)),
//    //                        Math.min(length / 2, Math.max(-length / 2, newPos.y))
//    //                    )
//                        newPos
//                }
//
//                val t1 = System.currentTimeMillis()
//                println(s"Iteration ${i + 1}/$iterations, ${t1 - t0} ms")
//
//                //            modifiedGraph.vertices.foreach(println)
//                modifiedGraph.checkpoint
//                modifiedGraph
//            }
//
//            Pajek.dump(Graph.fromSpark(computedGraph), outFilePath)
//
//        }
    override def start(sc: SparkContext, inFilePath: String): Graph[Point2] = {

    }

    override def run(iteration: Int, graph: Graph[Point2]): Graph[Point2] = {

    }

    override def end(graph: Graph[Point2], outFilePath: String): Unit = {

    }
}