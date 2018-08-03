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

object FruchtermanReingoldMutable extends Layouter[MutableGraph] {

    class FRNode(var pos: Point2, var displacement: Vec2)

    private val width = FruchtermanReingoldUtils.width
    private val length = FruchtermanReingoldUtils.length
    private val initialTemperature = FruchtermanReingoldUtils.initialTemperature

    private val area = width * length
    private var k: Double = 0.0
    private var iterations: Int = 0

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

    override def start(sc: SparkContext, inFilePath: String, iterations: Int): MutableGraph[Point2] = {
        // Place vertices at random
        val parsedGraph = Pajek.parse(inFilePath)
            .map { _ => new Point2(Math.random() * width, Math.random() * length) }

        // Create the mutable graph
        val initialGraph = MutableGraph.fromImmutable(parsedGraph)

        this.k = Math.sqrt(area / initialGraph.vertices.length) // Optimal pairwise distance
        this.iterations = iterations

        new MutableGraph[Point2](initialGraph.vertices, initialGraph.edges)
    }

    override def run(i: Int, g: MutableGraph[Point2]): MutableGraph[Point2] = {
        val (vertices, edges) = (g.vertices.map(pos => new FRNode(pos, Vec2.zero)), g.edges)

        val t = temperature(i, iterations)

        for (v <- vertices) {
            v.displacement = Vec2.zero
        }

        for (i <- vertices.indices; j <- vertices.indices) {
            if (i != j) {
                val (v1, v2) = (vertices(i), vertices(j))
                val delta = v1.pos - v2.pos
                val displacement = delta.normalize * repulsiveForce(k, delta.length)
                v1.displacement += displacement
            }
        }

        for ((v1Index, v2Index) <- edges) {
            val delta = vertices(v1Index - 1).pos - vertices(v2Index - 1).pos
            val displacement = delta.normalize * attractiveForce(k, delta.length)
            vertices(v1Index - 1).displacement -= displacement
            vertices(v2Index - 1).displacement += displacement
        }

        for (v <- vertices) {
            v.pos += v.displacement.normalize * Math.min(t, v.displacement.length)
        }

        new MutableGraph[Point2](vertices.map(v => v.pos), edges)
    }

    override def end(g: MutableGraph[Point2], outFilePath: String): Unit = {
        val graph = g match {
            case MutableGraph(vertices, edges) => new MutableGraph(vertices, edges)
            case _ => throw new IllegalArgumentException
        }
        Pajek.dump(graph.toImmutable, outFilePath)
    }
}

object FruchtermanReingoldSpark extends Layouter[SparkGraph] {
    private val width = FruchtermanReingoldUtils.width
    private val length = FruchtermanReingoldUtils.length
    private val initialTemperature = FruchtermanReingoldUtils.initialTemperature

    private val area = width * length
    private var k: Double = 0.0
    private var iterations: Int = 0

    private def repulsiveForce(k: Double, x: Double) = FruchtermanReingoldUtils.repulsiveForce(k, x)
    private def attractiveForce(k: Double, x: Double) = FruchtermanReingoldUtils.attractiveForce(k, x)
    private def temperature(currIter: Int, maxIter: Int) = FruchtermanReingoldUtils.temperature(currIter, maxIter)

    override def start (sc: SparkContext, inFilePath: String, iterations: Int): SparkGraph[Point2] = {
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

        this.k = Math.sqrt(area / initialGraph.numVertices) // Optimal pairwise distance
        this.iterations = iterations

        new SparkGraph[Point2](initialGraph)
    }

    override def run (i: Int, g: SparkGraph[Point2]): SparkGraph[Point2] = {
        val graph = g.graph
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
//              This seems to be useless and aesthetically unpleasant...
//                  val framedPos = new Point2(
//                      Math.min(width / 2, Math.max(-width / 2, newPos.x)),
//                      Math.min(length / 2, Math.max(-length / 2, newPos.y))
//                  )
                newPos
        }

        modifiedGraph.checkpoint
        new SparkGraph[Point2](modifiedGraph)
    }

    override def end(g: SparkGraph[Point2], outFilePath: String): Unit = {
        val graph = g match {
            case SparkGraph(graph: XGraph[Point2, Null]) => graph
            case _ => throw new IllegalArgumentException
        }
        Pajek.dump(ImmutableGraph.fromSpark(graph), outFilePath)
    }
//    override def start[T <: Graph[Point2]](sc: SparkContext, inFilePath: String, iterations: Int): T = ???
//
//    override def run[T <: Graph[Point2]](iteration: Int, graph: T): T = ???
//
//    override def end[T <: Graph[Point2]](graph: T, outFilePath: String): Unit = ???
}