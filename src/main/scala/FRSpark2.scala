import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, PartitionStrategy, VertexId, VertexRDD, Graph => XGraph}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.storage.StorageLevel

object FRSpark2 extends FRData with Layouter[Point2, SparkGraph] {

    private val area = width * length
    private var k: Double = 0.0
    private var iterations: Int = 0
    private val checkpointInterval = 1
    private val nCentroids = 300
    private val nPartitions = 4

    private var centroids: Array[Point2] = Array()
    private var center: Point2 = Point2.zero

    private var sparkContext: SparkContext = _

    def closestCentroid(pos: Point2): (Int, Point2, Double) = {
        val (closestPos, closestDist, closestId) =
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
        (closestId, closestPos, closestDist)
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
            ).repartition(nPartitions),
            sc.parallelize(
                parsedGraph.edges
                    .map { case (u, v) => Edge(u, v, null) }
            )
        )

        println(sc.defaultParallelism, sc.defaultMinPartitions, initialGraph.vertices.getNumPartitions)

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

        val verticesWithCentroid = graph.vertices
            .map {
                case (id, pos) =>
                    val (cId, cPos, dist) = closestCentroid(pos)
                    ((id, pos), (cId, cPos, dist))
            }

        val repulsionDisplacements = verticesWithCentroid
            .map {
                case ((id, pos), (_, cPos, _)) =>
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
            .reduceByKey(_ + _)

        // Sum the repulsion and attractive displacements
        val sumDisplacements = repulsionDisplacements
            .union(attractiveDisplacements)
//            .union(gravityDisplacements)
            .union(repulsionCenter)
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
                newPos
        }

        val verticesCentroidDistribution = verticesWithCentroid
            .map {
                case ((vId, vPos), (cId, cPos, dist)) =>
                    (cId, 1)
            }
            .reduceByKey(_ + _)
            .collectAsMap()

        centroids = verticesWithCentroid
            .map {
                case ((vId, vPos), (cId, cPos, dist)) =>
                    (cId, vPos)
            }
            .reduceByKey(_ + _)
            .map { case (id, pos) => pos / verticesCentroidDistribution(id) }
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

        if (i % checkpointInterval == 0) {
            modifiedGraph.checkpoint()
        }

        new SparkGraph[Point2](modifiedGraph)
    }

    override def end(g: SparkGraph[Point2], fs: FileSystem,  outFilePath: String): Unit = {
        Pajek.dump(ImmutableGraph.fromSpark(g.graph), fs, outFilePath)
    }
}