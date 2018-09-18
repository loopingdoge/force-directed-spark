import org.apache.spark.{SparkContext, graphx}
import org.apache.spark.graphx.{Edge, GraphLoader, PartitionStrategy, VertexId, VertexRDD, Graph => XGraph}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.storage.StorageLevel

object FRSpark2 extends FRData with Layouter[Point2, SparkGraph] {

    private val area = width * length
    private var k: Double = 0.0
    private var iterations: Int = 0
    private val checkpointInterval = 1
    private val nCentroids = 3000
    private val nPartitions = 4

    private var centroids: Array[Point2] = Array()
    private var center: Point2 = Point2.zero

    private var sparkContext: SparkContext = _

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

        sparkContext = sc

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

        centroids = initialGraph.vertices
            .takeSample(withReplacement = false, nCentroids)
            .map { case (_, pos) => pos }

        center = initialGraph.vertices
            .map {
                case (id, pos) =>
                    pos
            }
            .reduce {
                case (pos1, pos2) =>
                    pos1 + pos2
            } / initialGraph.numVertices.toDouble

        sparkContext.broadcast(centroids)
        sparkContext.broadcast(center)

        new SparkGraph[Point2](initialGraph)
    }

    override def run (i: Int, g: SparkGraph[Point2]): SparkGraph[Point2] = {
        val graph = g.graph
        val t = temperature(i, iterations)

        println(graph.vertices.getNumPartitions, graph.edges.getNumPartitions)

        val verticesWithCentroid = graph.vertices
            .map {
                case (id, pos) =>
                    val (cId, cPos) = closestCentroid(pos)
                    ((id, pos), (cId, cPos))
            }
            .cache()

        verticesWithCentroid.take(1)


        val repulsionDisplacements = verticesWithCentroid
            .map {
                case ((id, pos), (_, cPos)) =>
                    val delta = cPos - pos
                    val displacement = delta.normalize * repulsiveForce(k, delta.length) * 100
                    (id, displacement)
            }

        val repulsionCenter = graph.vertices
            .map {
                case (id, pos) =>
                    val delta = center - pos
                    val displacement = delta.normalize * repulsiveForce(k, delta.length) * 2
                    (id, displacement)
            }

//        val gravityDisplacements: RDD[(VertexId, Vec2)] = vertices
//            .map {
//                case (id, pos) =>
//                    val displacement = gravityForce(pos, k)
//                    (id, displacement)
//            }

        val attractiveDisplacements: RDD[(VertexId, Vec2)] = graph.triplets
            // Calculate the displacement for every edge
            // Flatten the list containing 2 pairs (one for each node of the edge)
            .flatMap { t =>
                val delta = t.srcAttr - t.dstAttr
                val displacement = delta.normalize * attractiveForce(k, delta.length)
                Vector((t.srcId, -displacement), (t.dstId, displacement))
            }

        // Sum the repulsion and attractive displacements
        val sumDisplacements = repulsionDisplacements
            .union(attractiveDisplacements)
//            .union(gravityDisplacements)
            .union(repulsionCenter)
            .reduceByKey(_ + _)

        // Obtain a new graph by summing the displacements to the node current position
        // while doing some weird stuff to align them and prevent them to go
        // outside of the frame area
        val modifiedGraph = graph.joinVertices(sumDisplacements) {
            case (id, pos, displ) =>
                val newPos = pos + displ.normalize * Math.min(t, displ.length)
                newPos
        }

//        val modifiedGraph = graph.mapVertices {
//            case (id, pos) =>
//                val vDispl = Vec2.zero
//                val newPos = pos + vDispl.normalize * Math.min(t, vDispl.length)
//                newPos
//        }

        val verticesCentroidDistribution = verticesWithCentroid
            .map {
                case ((vId, vPos), (cId, cPos)) =>
                    (cId, 1)
            }
            .reduceByKey(_ + _)

        centroids = verticesWithCentroid
            .map {
                case ((vId, vPos), (cId, cPos)) =>
                    (cId, vPos)
            }
            .reduceByKey(_ + _)
            .join(verticesCentroidDistribution)
            .map { case (id, (pos, count)) => pos / count }
            .collect()

        center = graph.vertices
            .map {
                case (_, pos) =>
                    pos
            }
            .reduce {
                case (pos1, pos2) =>
                    pos1 + pos2
            } / graph.numVertices.toDouble

        sparkContext.broadcast(centroids)
        sparkContext.broadcast(center)

        modifiedGraph.vertices.collect()

        if (i % checkpointInterval == 0) {
            modifiedGraph.checkpoint()
        }

        new SparkGraph[Point2](modifiedGraph)
    }

    override def end(g: SparkGraph[Point2], fs: FileSystem,  outFilePath: String): Unit = {
        Pajek.dump(ImmutableGraph.fromSpark(g.graph), fs, outFilePath)
    }
}