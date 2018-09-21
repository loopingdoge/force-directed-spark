import org.apache.spark._
import org.apache.spark.graphx.{Edge, PartitionStrategy, VertexId, Graph => XGraph}
import org.apache.spark.rdd.RDD

import org.apache.hadoop.fs.FileSystem
import scala.collection.immutable

object FA2Spark2 extends FA2Data with Layouter[(Point2, Int), SparkGraph] {

    private var speed = 1.0
    private var speedEfficiency = 1.0
    private var iterations = 0

    private var nVertices: Int = _
    private var nodesDispls: org.apache.spark.rdd.RDD[(VertexId, (Vec2, Vec2))] = _
    private var outboundAttractionCompensation = 0.0
    
    private var centroids: Array[(Point2, Int)] = Array()
    private var center: Point2 = Point2.zero

    def closestCentroid(pos: Point2): (VertexId, Point2, Int) = {
        val (closestPos, closestMass, _, closestId) =
            centroids
            .zipWithIndex
            .foldLeft((Point2.zero, 0, Double.MaxValue, -1)) {
                case ((accPos, accMass, accDist, accId), ((cPos, cMass), cId)) =>
                    val dist = (cPos - pos).length
                    if (dist < accDist) {
                        (cPos, cMass, dist, cId)
                    } else {
                        (accPos, accMass, accDist, accId)
                    }
            }
        (closestId, closestPos, closestMass)
    }

    override def start(sc: SparkContext, fs: FileSystem,  inFilePath: String, iterations: Int): SparkGraph[(Point2, Int)] = {
        // Place vertices at random
        val parsedGraph = Parser.parse(fs, inFilePath)
                .map { _ => Point2.random }

        this.nVertices = parsedGraph.vertices.length

        val initialGraph = XGraph(
            sc.parallelize(
                parsedGraph.vertices
                    .zipWithIndex
                    .map { case (v, i) => 
                        val mass = parsedGraph.edges
                            .filter { case (u, w) => u == i || w == i }
                            .length
                        (i.toLong, (v, mass))
                    }
            ),
            sc.parallelize(
                parsedGraph.edges
                    .map { case (u, v) => Edge(u, v, null) }
            )
        ).partitionBy(PartitionStrategy.EdgePartition2D)

        this.nodesDispls = initialGraph.vertices.map { case(id, _) => (id, (Vec2.zero, Vec2.zero)) }

        this.outboundAttractionCompensation =
            if (outboundAttractionDistribution) {
                initialGraph.vertices.map {
                    case (i, (pos, mass)) => mass
                }.sum / this.nVertices
            } else {
                1.0
            }
            
        this.iterations = iterations

        centroids = initialGraph.vertices
            .takeSample(withReplacement = false, (this.nVertices * 0.025).toInt)
            .map { case (_, (pos, mass)) => (pos, mass) }

        new SparkGraph[(Point2, Int)](initialGraph)
    }

    override def run(i: Int, g: SparkGraph[(Point2, Int)]): SparkGraph[(Point2, Int)] = {
        val graph = g.graph

        graph.persist()
        graph.checkpoint()

        center = graph.vertices
                .fold((0, (Point2.zero, 0))) {
                    case ((_, (pos1, _)), (_, (pos2, _))) =>
                        (0, (pos1 + pos2, 0))
                }._2._1 / graph.numVertices.toDouble

        val verticesWithCentroid = graph.vertices
            .mapValues { v =>
                val (pos, mass) = v
                val (cId, cPos, cMass) = closestCentroid(pos)
                (pos, mass, (cId, cPos, cMass))
            }
            .persist()
        
        val repulsiveForces = verticesWithCentroid
            .mapValues { v =>
                val (pos, mass, (_, cPos, cMass)) = v
                val delta = cPos - pos
                val (_, displacement) = repulsiveForce(new FANode(cPos, cMass), new FANode(pos, mass))                
                displacement
            }

        val gravityForces = graph.vertices.map {
            case (id, (pos, mass)) =>
                val displacement = gravityForce(new FANode(pos, mass))
                (id, displacement)
        }

        val attractiveForces = graph.triplets
            .flatMap( t => {
                val n1 = t.srcAttr
                val n2 = t.dstAttr
                val (d1, d2) = attractiveForce(
                    new FANode(n1._1, n1._2),
                    new FANode(n2._1, n2._2),
                    this.outboundAttractionCompensation
                ) 
                Vector( (t.srcId, d1), (t.dstId, d2) )
            })
            .reduceByKey(_ + _)

        // Sum the repulsion, attraction and gravity displacements
        val sumDisplacements = repulsiveForces
            .union(gravityForces)
            .union(attractiveForces)
            .reduceByKey(_ + _)

        this.nodesDispls = this.nodesDispls.join(sumDisplacements)
            .map {
                case (id, ((displ, oldDispl), newDispl)) =>
                    (id, (newDispl, displ))
            }

        var (totalSwinging: Double, totalEffectiveTraction: Double) = graph.vertices
            .join(this.nodesDispls)
            .map {
                case (id, ((pos, mass), (displ, oldDispl))) =>
                    val swinging = (oldDispl - displ).length * mass
                    val effectiveTraction = 0.5 * mass * (oldDispl + displ).length
                    (swinging, effectiveTraction)
            }
            .reduce {
                case ( (sw1, et1), (sw2, et2) ) => {
                    (sw1 + sw2, et1 + et2)
                }
            }
            
        // Optimize jitter tolerance
        // The 'right' jitter tolerance for this network. Bigger networks need more tolerance.
        // Denser networks need less tolerance. Totally empiric.
        val estimatedOptimalJitterTolerance = 0.05 * Math.sqrt(this.nVertices)
        val minJitter = Math.sqrt(estimatedOptimalJitterTolerance)
        val maxJitter = 10
        var jitter =
            jitterTolerance * Math.max(
                minJitter,
                Math.min(
                    maxJitter,
                    (estimatedOptimalJitterTolerance * totalEffectiveTraction) / Math.pow(this.nVertices, 2)
                )
            )
        val minSpeedEfficiency = 0.05

        // Protection against erratic behavior
        if (totalSwinging / totalEffectiveTraction > 2.0) {
            if (this.speedEfficiency > minSpeedEfficiency) {
                this.speedEfficiency = this.speedEfficiency * 0.5
            }
            jitter = Math.max(jitter, jitterTolerance)
        }

        val targetSpeed = (jitter * this.speedEfficiency * totalEffectiveTraction) / totalSwinging

        // Speed efficiency is how the speed really corresponds to the swinging vs. convergence tradeoff
        // We adjust it slowly and carefully
        if (totalSwinging > jitter * totalEffectiveTraction) {
            if (this.speedEfficiency > minSpeedEfficiency) {
                this.speedEfficiency = this.speedEfficiency * 0.7
            }
        } else if (this.speed < 1000) {
            this.speedEfficiency = this.speedEfficiency * 1.3
        }

        // But the speed shoudn't rise too much too quickly, since it would make the convergence drop dramatically.
        val maxRise = 0.5   // Max rise: 50%
        this.speed = this.speed + Math.min(targetSpeed - this.speed, maxRise * this.speed)

        // Apply forces
        val modifiedGraph = graph
            .joinVertices(this.nodesDispls) {
                case (id, (pos, mass), (displ, oldDispl)) =>
                    val swinging = mass * (oldDispl - displ).length
                    val factor = speed / (1.0 + Math.sqrt(speed * swinging))
                    val newPos = pos + (displ * factor)
                    (newPos, mass)
            }

        val centroidsDegree = verticesWithCentroid
            .map {
                case (id, (vPos, vMass, (cId, cPos, cMass))) =>
                    (cId, 1)
            }
            .reduceByKey(_ + _)
            .collectAsMap()

        centroids = verticesWithCentroid
            .map {
                case (vId, (vPos, vMass, (cId, cPos, cMass))) =>
                    (cId, (vPos, cMass))
            }
            .reduceByKey{ (a, b) => 
                val (pos1, mass) = a
                val (pos2, _) = b
                (pos1 + pos2, mass)
            }
            .collect()
            .map {
                case (id, (pos, mass)) => (pos / centroidsDegree(id), mass)
            }

        new SparkGraph[(Point2, Int)](modifiedGraph)
    }

    override def end(g: SparkGraph[(Point2, Int)], fs: FileSystem,  outFilePath: String): Unit = {
        val outGraph = g.graph.mapVertices { case (id, (pos, mass)) => pos }
        Pajek.dump(ImmutableGraph.fromSpark(outGraph), fs, outFilePath)
    }
}