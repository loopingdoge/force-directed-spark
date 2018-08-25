import org.apache.spark._
import org.apache.spark.graphx.{Edge, VertexId, Graph => XGraph}
import org.apache.spark.rdd.RDD

import scala.collection.immutable

object FA2Spark extends FA2Data with Layouter[MutableGraph] {

    private var speed = 1.0
    private var speedEfficiency = 1.0
    private var iterations = 0

    private var nodesDx: RDD[(VertexId, Vec2)] = _
    private var nodesOldDx: RDD[(VertexId, Vec2)] = _
    private var nodesMass: RDD[(VertexId, Long)] = _
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
                (
                    i,
                    initialGraph.edges.filter {
                        case e => e.srcId == i - 1 || e.dstId == i - 1
                    }.count.toLong
                )
            }.cache

        this.outboundAttractionCompensation =
            if (outboundAttractionDistribution) {
                this.nodesMass.map { case (i, mass) => mass }.sum / initialGraph.vertices.count
            } else {
                1.0
            }

        this.iterations = iterations

        new SparkGraph[Point2](initialGraph)
    }

    override def run(i: Int, g: SparkGraph[Point2]): SparkGraph[Point2] = {

        val vertices = g.vertices.zipWithIndex.map {
            case (p: Point2, i: Int) => {
                val nodeMass = g.edges.foldLeft(0)((acc, edge) => {
                    if (edge._1 == i || edge._2 == i) acc + 1 else acc
                })
                new FANode(p, nodeMass)
            }
        }
        
        val edges = g.edges

        for (i <- vertices.indices) {
            this.nodesOldDx(i) = this.nodesDx(i)
            this.nodesDx(i) = Vec2.zero
        }

        for (
            i <- vertices.indices;
            j <- i + 1 until vertices.length
        ) {
            val (d1, d2) = repulsiveForce(vertices(i), vertices(j))
            this.nodesDx(i) = this.nodesDx(i) + d1
            this.nodesDx(j) = this.nodesDx(j) + d2
        }

        for (i <- vertices.indices) {
            val displacement = gravityForce(vertices(i))
            this.nodesDx(i) = this.nodesDx(i) + displacement
        }

        edges.foreach { case (index1, index2) => {

                val (d1, d2) = attractiveForce(vertices(index1), vertices(index2), this.outboundAttractionCompensation)

                this.nodesDx(index1) = this.nodesDx(index1) + d1
                this.nodesDx(index2) = this.nodesDx(index2) + d2
            }
        }

        // Auto adjust speed
        var totalSwinging = 0.0             // How much irregular movement
        var totalEffectiveTraction = 0.0    // Hom much useful movement

        for (i <- vertices.indices) {
            val swinging = (this.nodesOldDx(i) - this.nodesDx(i)).length
            totalSwinging += vertices(i).mass * swinging
            totalEffectiveTraction += 0.5 * vertices(i).mass * (this.nodesOldDx(i) + this.nodesDx(i)).length
        }

        // Optimize jitter tolerance
        // The 'right' jitter tolerance for this network. Bigger networks need more tolerance.
        // Denser networks need less tolerance. Totally empiric.
        val estimatedOptimalJitterTolerance = 0.05 * Math.sqrt(vertices.length)
        val minJitter = Math.sqrt(estimatedOptimalJitterTolerance)
        val maxJitter = 10
        var jitter =
            jitterTolerance * Math.max(
                minJitter,
                Math.min(
                    maxJitter,
                    (estimatedOptimalJitterTolerance * totalEffectiveTraction) / Math.pow(vertices.length, 2)
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
        for (i <- vertices.indices) {
            // Adaptive auto-speed: the speed of each node is lowered
            // when the node swings.
            val swinging = vertices(i).mass * (this.nodesOldDx(i) - this.nodesDx(i)).length
            val factor = this.speed / (1.0 + Math.sqrt(this.speed * swinging))
            vertices(i).pos = vertices(i).pos + (this.nodesDx(i) * factor)
        }
        var culo = new MutableGraph[Point2](vertices.map(v => v.pos), edges)
        
        new MutableGraph[Point2](vertices.map(v => v.pos), edges)
    }

    override def end(g: SparkGraph[Point2], outFilePath: String): Unit = {
        Pajek.dump(g.fromSpark(g.graph), outFilePath)
    }

}