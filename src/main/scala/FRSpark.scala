import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph => XGraph, Edge, VertexId}
import org.apache.spark.rdd.RDD

object FRSpark extends FRData with Layouter[SparkGraph] {

    private val area = width * length
    private var k: Double = 0.0
    private var iterations: Int = 0

    override def start (sc: SparkContext, inFilePath: String, iterations: Int): SparkGraph[Point2] = {
        // Place vertices at random
        val parsedGraph = Parser.parse(inFilePath)
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

        new SparkGraph[Point2](initialGraph)
    }

    override def run (i: Int, g: SparkGraph[Point2]): SparkGraph[Point2] = {
        val graph = g.graph
        val t = temperature(i, iterations)

        val repulsionDisplacements: RDD[(VertexId, Vec2)] = graph.vertices
            // Generate every possible node pairs
            .cartesian(graph.vertices)
            // Remove the pairs having the same ID
            .filter {
                case ((id1, _), (id2, _)) => id1 != id2
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

        val gravityDisplacements: RDD[(VertexId, Vec2)] = graph.vertices
            .map {
                case (id, pos) =>
                    val displacement = gravityForce(pos, k)
                    (id, displacement)
            }

        val attractiveDisplacements: RDD[(VertexId, Vec2)] = graph.triplets
            // Calculate the displacement for every edge
            // Flatten the list containing 2 pairs (one for each node of the edge)
            .flatMap { t =>
                val delta = t.srcAttr - t.dstAttr
                val displacement = delta.normalize * attractiveForce(k, delta.length)
                Vector((t.srcId, -displacement), (t.dstId, displacement))
            }
            .reduceByKey(_ + _)

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
//                This seems to be useless and aesthetically unpleasant...
//                val framedPos = new Point2(
//                  Math.min(width / 2, Math.max(-width / 2, newPos.x)),
//                  Math.min(length / 2, Math.max(-length / 2, newPos.y))
//                )
                newPos
        }

        modifiedGraph.checkpoint()
        new SparkGraph[Point2](modifiedGraph)
    }

    override def end(g: SparkGraph[Point2], outFilePath: String): Unit = {
        Pajek.dump(ImmutableGraph.fromSpark(g.graph), outFilePath)
    }

}