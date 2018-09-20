import org.apache.spark.{SparkContext, graphx}
import org.apache.spark.graphx.{Edge, PartitionStrategy, VertexId, Graph => XGraph}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.FileSystem

object FRSpark2 extends FRData with Layouter[Point2, SparkGraph] {

    private val area = width * length
    private var k: Double = 0.0
    private var iterations: Int = 0

    private var centroids: Array[Point2] = Array()
    private var center: Point2 = Point2.zero

    def closestCentroid(pos: Point2): (Int, Point2) = {
        val (closestPos, _, closestId) =
            centroids
            .zipWithIndex
            .foldLeft((Point2.zero, Double.MaxValue, -1)) {
                case ((accPos, accDist, accId), (cPos, cId)) =>
                    val dist = (cPos - pos).length
                    if (dist < accDist) {
                        (cPos, dist, cId)
                    } else {
                        (accPos, accDist, accId)
                    }
            }
        (closestId, closestPos)
    }

    override def start (sc: SparkContext, fs: FileSystem,  inFilePath: String, iterations: Int): SparkGraph[Point2] = {
        // Place vertices at random
        val parsedGraph = Parser.parse(fs, inFilePath)
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
        ).partitionBy(PartitionStrategy.EdgePartition2D)

        this.k = Math.sqrt(area / initialGraph.numVertices) // Optimal pairwise distance
        this.iterations = iterations

        centroids = initialGraph.vertices
            .takeSample(withReplacement = false, (initialGraph.numVertices * 0.025).toInt)
            .map { case (_, pos) => pos }

        new SparkGraph[Point2](initialGraph)
    }

    override def run (i: Int, g: SparkGraph[Point2]): SparkGraph[Point2] = {
        val graph = g.graph
        val t = temperature(i, iterations)

        graph.persist()
        graph.checkpoint()

        center = graph.vertices
                .fold((0, Point2.zero)) {
                    case ((_, pos1), (_, pos2)) =>
                        (0, pos1 + pos2)
                }._2 / graph.numVertices.toDouble

        val verticesWithCentroid = graph.vertices
            .mapValues { pos =>
                val (cId, cPos) = closestCentroid(pos)
                (pos, (cId, cPos))
            }
            .persist()

        val repulsionDisplacements = verticesWithCentroid
            .mapValues { v =>
                val (pos, (_, cPos)) = v
                val delta = cPos - pos
                val displacement = delta.normalize * repulsiveForce(k, delta.length) * 100
                displacement
            }

        val repulsionCenter = graph.vertices
            .mapValues {
                pos =>
                    val delta = center - pos
                    val displacement = delta.normalize * repulsiveForce(k, delta.length) * 2
                    displacement
            }

        val attractiveDisplacements = graph.triplets
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
            .union(repulsionCenter)
            .union(attractiveDisplacements)
            .reduceByKey(_ + _)


        // Obtain a new graph by summing the displacements to the node current position
        val modifiedGraph = graph.joinVertices(sumDisplacements) {
            case (id, pos, displ) =>
                val newPos = pos + displ.normalize * Math.min(t, displ.length)
                newPos
        }

        val verticesCentroidDistribution = verticesWithCentroid
            .map {
                case (id, (vPos, (cId, cPos))) =>
                    (cId, 1)
            }
            .reduceByKey(_ + _)
            .collectAsMap()

        centroids = verticesWithCentroid
            .map {
                case (vId, (vPos, (cId, cPos))) =>
                    (cId, vPos)
            }
            .reduceByKey(_ + _)
            .collect()
            .map {
                case (id, pos) => pos / verticesCentroidDistribution(id)
            }

        new SparkGraph[Point2](modifiedGraph)
    }

    override def end(g: SparkGraph[Point2], fs: FileSystem,  outFilePath: String): Unit = {
        Pajek.dump(ImmutableGraph.fromSpark(g.graph), fs, outFilePath)
    }
}