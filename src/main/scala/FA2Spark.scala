import org.apache.spark._
import org.apache.spark.graphx.{Edge, VertexId, Graph => XGraph}
import org.apache.spark.rdd.RDD

import org.apache.hadoop.fs.FileSystem
import scala.collection.immutable

object FA2Spark extends FA2Data with Layouter[(Point2, Int), SparkGraph] {

    private var speed = 1.0
    private var speedEfficiency = 1.0
    private var iterations = 0

    private var nVertices: Int = _
    private var nodesOldDx: scala.collection.Map[VertexId, Vec2] = _
    private var getNodeMass: (VertexId) => Int = _
    private var outboundAttractionCompensation = 0.0
    
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
        )

        this.nodesOldDx = Map( (0 until this.nVertices).map(i => i.toLong -> Vec2.zero) : _* )

        this.outboundAttractionCompensation =
            if (outboundAttractionDistribution) {
                initialGraph.vertices.map {
                    case (i, (pos, mass)) => mass
                }.sum / this.nVertices
            } else {
                1.0
            }
        print(s"${this.outboundAttractionCompensation} \n\n")
        this.iterations = iterations

        new SparkGraph[(Point2, Int)](initialGraph)
    }

    override def run(i: Int, g: SparkGraph[(Point2, Int)]): SparkGraph[(Point2, Int)] = {
        val graph = g.graph

        val repulsiveForces: RDD[(VertexId, Vec2)] = graph.vertices
            // Generate every possible node pairs
            .cartesian(graph.vertices)
            .filter { case ((id1, _), (id2, _)) => id1 < id2 }
            .flatMap {
                case ((id1, (pos1, mass1)), (id2, (pos2, mass2))) =>
                    val (d1, d2) = repulsiveForce(new FANode(pos1, mass1), new FANode(pos2, mass2))                
                    Vector( (id1, d1), (id2, d2) )
            }
            .reduceByKey((a: Vec2, b: Vec2) => a + b)

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
            .reduceByKey((a: Vec2, b: Vec2) => a + b)

        // Sum the repulsion, attraction and gravity displacements
        val sumDisplacements = repulsiveForces
            .union(gravityForces)
            .union(attractiveForces)
            .reduceByKey(_ + _)
            // Collect as a Map in order to get the displacement given the nodeID
            .collectAsMap

        var (totalSwinging: Double, totalEffectiveTraction: Double) = graph.vertices
            .map {
                case (id, (pos, mass)) =>
                    val nodeDx = sumDisplacements(id)
                    val nodeOldDx = this.nodesOldDx(id)
                    val swinging = (nodeOldDx - nodeDx).length * mass
                    val effectiveTraction = 0.5 * mass * (nodeOldDx + nodeDx).length
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
            .mapVertices {
                case (id, (pos, mass)) =>
                    val nodeDx = sumDisplacements(id)
                    val swinging = mass * (this.nodesOldDx(id) - nodeDx).length
                    val factor = this.speed / (1.0 + Math.sqrt(this.speed * swinging))
                    val newPos = pos + (nodeDx * factor)
                    (newPos, mass)
            }  

        this.nodesOldDx = sumDisplacements

        modifiedGraph.checkpoint()
        new SparkGraph[(Point2, Int)](modifiedGraph)
    }

    override def end(g: SparkGraph[(Point2, Int)], fs: FileSystem,  outFilePath: String): Unit = {
        val outGraph = g.graph.mapVertices { case (id, (pos, mass)) => pos }
        Pajek.dump(ImmutableGraph.fromSpark(outGraph), fs, outFilePath)
    }
}