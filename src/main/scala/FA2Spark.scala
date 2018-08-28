import org.apache.spark._
import org.apache.spark.graphx.{Edge, VertexId, Graph => XGraph}
import org.apache.spark.rdd.RDD

import scala.collection.immutable

object FA2Spark extends FA2Data with Layouter[SparkGraph] {

    private var speed = 1.0
    private var speedEfficiency = 1.0
    private var iterations = 0

    private var nodesDx: RDD[(VertexId, Vec2)] = _
    private var nodesOldDx: RDD[(VertexId, Vec2)] = _
    private var nodesMass: RDD[(VertexId, Int)] = _
    private var getNodeMass: (VertexId) => Int = _
    private var outboundAttractionCompensation = 0.0
    
    override def start(sc: SparkContext, inFilePath: String, iterations: Int): SparkGraph[Point2] = {
        // Place vertices at random
        val parsedGraph = Parser.parse(inFilePath)
                .map { _ => Point2.random }

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

        this.nodesDx = initialGraph.vertices.map { case (i, p) => (i, Vec2.zero) }.cache
        this.nodesOldDx = initialGraph.vertices.map { case (i, p) => (i, Vec2.zero) }.cache

        this.nodesMass = initialGraph.vertices
            .map { case (i, p) => 
                val mass = parsedGraph.edges
                    .filter { case (u, v) => u == i || v == i }
                    .length
                ( i, mass )
            }.cache

        this.outboundAttractionCompensation =
            if (outboundAttractionDistribution) {
                this.nodesMass.map { case (i, mass) => mass }.sum / initialGraph.vertices.count
            } else {
                1.0
            }

        // TODO Vedere dove metterlo
        this.getNodeMass = (nodeId: VertexId) => 
            this.nodesMass
                .filter { case (id, mass) => id == nodeId }
                .map { case (id, mass) => mass }
                .first

        this.iterations = iterations

        new SparkGraph[Point2](initialGraph)
    }

    override def run(i: Int, g: SparkGraph[Point2]): SparkGraph[Point2] = {

        val graph = g.graph

        val nVertices = graph.vertices.count

        this.nodesOldDx = this.nodesDx
        this.nodesDx = this.nodesDx.map { case (i, p) => (i, Vec2.zero) }.cache

        val getNodePos = (nodeId: VertexId) =>
            graph.vertices
            .filter { case (id, pos) => id == nodeId }
            .map { case (id, pos) => pos }
            .first

        val repulsiveForces: RDD[(VertexId, Vec2)] = graph.vertices
            // Generate every possible node pairs
            .cartesian(graph.vertices)
            .filter { case ((id1, _), (id2, _)) => id1 != id2 }
            .flatMap {
                case ((id1, pos1), (id2, pos2)) =>
                    val mass1 = this.getNodeMass(id1)
                    val mass2 =  this.getNodeMass(id2)

                    val (d1, d2) = repulsiveForce(new FANode(pos1, mass1), new FANode(pos2, mass2))

                    this.nodesDx = this.nodesDx
                        .map {
                            case (id, pos) =>
                                if (id == id1) (id, pos + d1)
                                else if (id == id2) (id, pos + d2)
                                else (id, pos)
                        }

                    Vector( (id1, d1), (id2, d2) )
            }
            .reduceByKey((a: Vec2, b: Vec2) => a + b)

        val gravityForces = graph.vertices.map {
            case (id, pos) => 
                val mass = this.getNodeMass(id)
                val displacement = gravityForce(new FANode(pos, mass))
                
                this.nodesDx = this.nodesDx
                    .map {
                        case (nId, pos) =>
                            if (nId == id) (nId, pos + displacement)
                            else (nId, pos)
                    }

                (id, displacement)
        }

        val attractiveForces = graph.edges
            .flatMap{
                case e =>
                    val id1 = e.srcId
                    val id2 = e.dstId
                    val mass1 = this.getNodeMass(id1)
                    val mass2 =  this.getNodeMass(id2)

                    val (d1, d2) = attractiveForce(
                        new FANode(getNodePos(id1), mass1),
                        new FANode(getNodePos(id1), mass2),
                        this.outboundAttractionCompensation
                    )

                    this.nodesDx = this.nodesDx
                        .map {
                            case (id, pos) =>
                                if (id == id1) (id, pos + d1)
                                else if (id == id2) (id, pos + d2)
                                else (id, pos)
                        }

                    Vector( (id1, d1), (id2, d2))
            }
            .reduceByKey((a: Vec2, b: Vec2) => a + b)

        var (totalSwinging: Double, totalEffectiveTraction: Double) = graph.vertices
            .map {
                case (id, pos) =>
                    val nodeDx = this.nodesDx
                        .filter { case (i, d) => i == id }
                        .map { case (i, d) => d }
                        .first

                    val nodeOldDx = this.nodesOldDx
                        .filter { case (i, d) => i == id }
                        .map { case (i, d) => d }
                        .first

                    val mass = this.getNodeMass(id)

                    val swinging = (nodeOldDx - nodeDx).length * mass
                    val effectiveTraction = 0.5 * mass * (nodeOldDx - nodeDx).length
                    (swinging, effectiveTraction)
            }
            .reduce {
                case ( (sw1, et1), (sw2, et2) ) => {
                    (sw1 + sw2, et1 + et2)
                }
            }
        // QUESTO TYPECHECKA PER CULO

        // Optimize jitter tolerance
        // The 'right' jitter tolerance for this network. Bigger networks need more tolerance.
        // Denser networks need less tolerance. Totally empiric.
        val estimatedOptimalJitterTolerance = 0.05 * Math.sqrt(nVertices)
        val minJitter = Math.sqrt(estimatedOptimalJitterTolerance)
        val maxJitter = 10
        var jitter =
            jitterTolerance * Math.max(
                minJitter,
                Math.min(
                    maxJitter,
                    (estimatedOptimalJitterTolerance * totalEffectiveTraction) / Math.pow(nVertices, 2)
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
        // TODO parallelizzarlo?
        val maxRise = 0.5   // Max rise: 50%
        this.speed = this.speed + Math.min(targetSpeed - this.speed, maxRise * this.speed)

        // // Apply forces
        // for (i <- vertices.indices) {
        //     // Adaptive auto-speed: the speed of each node is lowered
        //     // when the node swings.
        //     val swinging = vertices(i).mass * (this.nodesOldDx(i) - this.nodesDx(i)).length
        //     val factor = this.speed / (1.0 + Math.sqrt(this.speed * swinging))
        //     vertices(i).pos = vertices(i).pos + (this.nodesDx(i) * factor)
        // }
        //  TODO modificare graph
        val modifiedGraph = graph
            .mapVertices {
                case (id, pos) =>
                    val nodeDx = this.nodesDx
                        .filter { case (i, d) => i == id }
                        .map { case (i, d) => d }
                        .first

                    val nodeOldDx = this.nodesOldDx
                        .filter { case (i, d) => i == id }
                        .map { case (i, d) => d }
                        .first

                    val mass = this.getNodeMass(id)
                    val swinging = mass * (nodeOldDx - nodeDx).length
                    val factor = this.speed / (1.0 + Math.sqrt(this.speed * swinging))
                    val newPos = pos + (nodeDx * factor)
                    newPos
            }
        
         new SparkGraph[Point2](modifiedGraph)
    }

    override def end(g: SparkGraph[Point2], outFilePath: String): Unit = {
        Pajek.dump(ImmutableGraph.fromSpark(g.graph), outFilePath)
    }

}