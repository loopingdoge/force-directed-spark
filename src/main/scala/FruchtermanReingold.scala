import org.apache.spark._
import org.apache.spark.graphx.{Edge, VertexId, Graph => XGraph}
import org.apache.spark.rdd.RDD

class FRNode(var pos: Point2, var displacement: Vec2)

object FruchtermanReingoldUtils {

    val width = 300
    val length = 300
    val initialTemperature: Double = width / 10
    val gravity = 10

    def repulsiveForce(k: Double, x: Double): Double = {
        val divisor = if (x == 0) 0.001 else x
        Math.pow(k, 2) / divisor
    }

    def attractiveForce(k: Double, x: Double): Double = {
        val divisor = if (x == 0) 0.001 else x
        Math.pow(x, 2) / divisor
    }

    def gravityForce(pos: Point2, k: Double): Vec2 = {
        val distanceFromOriginLength: Double = pos.toVec.length
        if (distanceFromOriginLength > 0) {
            val factor = 0.01 * k * gravity * distanceFromOriginLength
            -(pos.toVec * factor) / distanceFromOriginLength
        } else {
            Vec2.zero
        }
    }

    // Inverse linear temperature decay
    def temperature(currIter: Int, maxIter: Int): Double = {
        -((currIter - maxIter).toDouble / maxIter) * initialTemperature
    }
}

object FruchtermanReingoldMutable extends Layouter[MutableGraph] {

    private val width = FruchtermanReingoldUtils.width
    private val length = FruchtermanReingoldUtils.length

    private val area = width * length
    private var k: Double = 0.0
    private var iterations: Int = 0

    private def repulsiveForce(k: Double, x: Double) = FruchtermanReingoldUtils.repulsiveForce(k, x)
    private def attractiveForce(k: Double, x: Double) = FruchtermanReingoldUtils.attractiveForce(k, x)
    private def gravityForce(pos: Point2, k: Double): Vec2 = FruchtermanReingoldUtils.gravityForce(pos, k)
    private def temperature(currIter: Int, maxIter: Int) = FruchtermanReingoldUtils.temperature(currIter, maxIter)

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
        val (vertices, edges) =
            (
                g.vertices.map(pos => new FRNode(pos, Vec2.zero)),
                g.edges
            )

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

        for (v <- vertices) {
            v.displacement += gravityForce(v.pos, k)
        }

        for ((v1Index, v2Index) <- edges) {
            val delta = vertices(v1Index).pos - vertices(v2Index).pos
            val displacement = delta.normalize * attractiveForce(k, delta.length)
            vertices(v1Index).displacement -= displacement
            vertices(v2Index).displacement += displacement
        }

        for (v <- vertices) {
            v.pos += v.displacement.normalize * Math.min(t, v.displacement.length)
        }

        new MutableGraph[Point2](vertices.map(v => v.pos), edges)
    }

    override def end(graph: MutableGraph[Point2], outFilePath: String): Unit = {
        Pajek.dump(graph.toImmutable, outFilePath)
    }
}

object FruchtermanReingoldSpark extends Layouter[SparkGraph] {
    private val width = FruchtermanReingoldUtils.width
    private val length = FruchtermanReingoldUtils.length

    private val area = width * length
    private var k: Double = 0.0
    private var iterations: Int = 0
    private var nodePairs: RDD[((VertexId, Point2), (VertexId, Point2))] = _

    private def repulsiveForce(k: Double, x: Double) = FruchtermanReingoldUtils.repulsiveForce(k, x)
    private def attractiveForce(k: Double, x: Double) = FruchtermanReingoldUtils.attractiveForce(k, x)
    private def temperature(currIter: Int, maxIter: Int) = FruchtermanReingoldUtils.temperature(currIter, maxIter)
    private def gravityForce(pos: Point2, k: Double): Vec2 = FruchtermanReingoldUtils.gravityForce(pos, k)

    override def start (sc: SparkContext, inFilePath: String, iterations: Int): SparkGraph[Point2] = {
        // Place vertices at random
        val parsedGraph = Pajek.parse(inFilePath)
            .map { _ => new Point2(Math.random() * width, Math.random() * length) }

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

        this.k = Math.sqrt(area / initialGraph.numVertices) // Optimal pairwise distance
        this.iterations = iterations

        this.nodePairs = initialGraph.vertices
            // Generate every possible node pairs
            .cartesian(initialGraph.vertices)
            // Remove the pairs having the same ID
            .filter {
                case ((id1, _), (id2, _)) => id1 != id2
            }
//            .cache()

        new SparkGraph[Point2](initialGraph)
    }

    override def run (i: Int, g: SparkGraph[Point2]): SparkGraph[Point2] = {
        val graph = g.graph
        val t = temperature(i, iterations)

        val repulsionDisplacements: RDD[(VertexId, Vec2)] =
            this.nodePairs
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
                Vector((t.srcId, -displacement), (t.dstId, displacement))
            }
            .reduceByKey(_ + _)

        val gravityDisplacements: RDD[(VertexId, Vec2)] = graph.vertices
            .map {
                case (id, pos) =>
                    val displacement = gravityForce(pos, k)
                    (id, displacement)
            }

        // Sum the repulsion and attractive displacements
        val sumDisplacements = repulsionDisplacements
            .union(attractiveDisplacements)
            .union(gravityDisplacements)
            .reduceByKey(_ + _)
            // Collect as a Map in order to get the displacement given the nodeID
            .collectAsMap

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

        modifiedGraph.checkpoint()
        new SparkGraph[Point2](modifiedGraph)
    }

    override def end(g: SparkGraph[Point2], outFilePath: String): Unit = {
        Pajek.dump(ImmutableGraph.fromSpark(g.graph), outFilePath)
    }

}