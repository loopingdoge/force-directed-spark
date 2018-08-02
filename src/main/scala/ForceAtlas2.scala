import org.apache.spark._
import org.apache.spark.graphx.{Edge, VertexId, Graph => XGraph}
import org.apache.spark.rdd.RDD

import scala.collection.immutable

class FANode (var pos: Point2, val mass: Int)

object ForceAtlas2 {

    val directed = false
    val barnesHutTheta = 1.2
    val edgeWeightInfluence = 0
    val gravity = 1.0
    val jitterTolerance = 1
    val scalingRatio = 2.0
    val adjustSizes = false
    val barnesHutOptimize = false
    val linLogMode = false
    val strongGravityMode = false
    val outboundAttractionDistribution = false
    // It should be the average node mass, but in our case it is always 1
    val outboundAttractionCompensation = 1

    def repulsiveForce(v1: FANode, v2: FANode, coefficient: Double): (Vec2, Vec2) = {
        val distance: Double = (v1.pos - v2.pos).length
        val factor: Double =
            if (distance > 0)
                coefficient * v1.mass * v2.mass / distance
                // coefficient * nodeMass * nodeMass / distance / distance
            else
                100 * v1.mass * v2.mass
        (v1.pos.toVec * factor, -v2.pos.toVec * factor)
    }

    def attractiveForce(source: FANode, target: FANode): (Vec2, Vec2) = {
        val distance: Double = (source.pos - target.pos).length
        val edgeWeight: Double = 1.0
        val factor = (linLogMode, outboundAttractionDistribution) match {
            case (true, true) =>
                -outboundAttractionCompensation * edgeWeight * Math.log(1 + distance) / distance / source.mass
            case (true, false) =>
                -outboundAttractionCompensation * edgeWeight * Math.log(1 + distance) / distance
            case (false, true) =>
                -outboundAttractionCompensation * edgeWeight / source.mass
            case (false, false) =>
                -outboundAttractionCompensation * edgeWeight
        }
        (source.pos.toVec * factor, -target.pos.toVec * factor)
    }

    def gravityForce(v: FANode): Vec2 = {
        val distanceFromOrigin: Double = v.pos.toVec.length
        val factor: Double =
            if (strongGravityMode)
                scalingRatio * v.mass * (gravity / scalingRatio)
            else
                scalingRatio * v.mass * (gravity / scalingRatio) / distanceFromOrigin
        -v.pos.toVec * factor
    }

    def runMutable(iterations: Int, inFilePath: String, outFilePath: String) {
        // Place vertices at random
        val parsedGraph = Pajek.parse(inFilePath)
                .map { _ => Point2.random }

        val graph = MutableGraph.fromImmutable(
            parsedGraph.map { (p, i) =>
                val nodeMass = parsedGraph.edges.foldLeft(0)((acc, edge) =>
                    if (edge._1 - 1 == i || edge._2 - 1 == i) acc + 1 else acc
                )
                new FANode(p, nodeMass)
            }
        )

        val nodesDx: Array[Vec2] = graph.vertices.map(_ => Vec2.zero )
        val nodesOldDx: Array[Vec2] = graph.vertices.map(_ => Vec2.zero )
        var speed = 1.0
        var speedEfficiency = 1.0

        val verticesCouples =
            for {
                i <- graph.vertices.indices
                j <- i + 1 until graph.vertices.length
            } yield (i, j)

        for (iteration <- 0 until iterations) {
            val t0 = System.currentTimeMillis()

            for (i <- graph.vertices.indices) {
                nodesOldDx(i) = nodesDx(i)
                nodesDx(i) = Vec2.zero
            }

            verticesCouples.foreach {
                case (index1, index2) =>
                    val (d1, d2) = repulsiveForce(graph.vertices(index1), graph.vertices(index2), scalingRatio)
                    nodesDx(index1) = nodesDx(index1) + d1
                    nodesDx(index2) = nodesDx(index2) + d2
            }

            println()
            for (i <- graph.vertices.indices) {
                val displacement = gravityForce(graph.vertices(i))
                nodesDx(i) = nodesDx(i) + displacement
            }

            println()
            graph.edges.foreach {
                case (index1, index2) =>
                    val (d1, d2) = attractiveForce(graph.vertices(index1 - 1), graph.vertices(index2 - 1))
                    nodesDx(index1 - 1) = nodesDx(index1 - 1) + d1
                    nodesDx(index2 - 1) = nodesDx(index2 - 1) + d2
            }

            println()

            // Auto adjust speed
            var totalSwinging = 0.0             // How much irregular movement
            var totalEffectiveTraction = 0.0    // Hom much useful movement

            for (i <- graph.vertices.indices) {
                val swinging = (nodesOldDx(i) - nodesDx(i)).length
                totalSwinging += graph.vertices(i).mass * swinging
                totalEffectiveTraction += 0.5 * graph.vertices(i).mass * (nodesOldDx(i) + nodesDx(i)).length
            }

            // Optimize jitter tolerance
            // The 'right' jitter tolerance for this network. Bigger networks need more tolerance.
            // Denser networks need less tolerance. Totally empiric.
            val estimatedOptimalJitterTolerance = 0.05 * Math.sqrt(graph.vertices.length)
            val minJitter = Math.sqrt(estimatedOptimalJitterTolerance)
            val maxJitter = 10
            var jitter =
                jitterTolerance * Math.max(
                    minJitter,
                    Math.min(
                        maxJitter,
                        (estimatedOptimalJitterTolerance * totalEffectiveTraction) / Math.pow(graph.vertices.length, 2)
                    )
                )
            val minSpeedEfficiency = 0.05

            // Protection against erratic behavior
            if (totalSwinging / totalEffectiveTraction > 2.0) {
                if (speedEfficiency > minSpeedEfficiency) {
                    speedEfficiency = speedEfficiency * 0.5
                }
                jitter = Math.max(jitter, jitterTolerance)
            }

            val targetSpeed = (jitter * speedEfficiency * totalEffectiveTraction) / totalSwinging

            if (totalSwinging > jitter * totalEffectiveTraction) {
                if (speedEfficiency > minSpeedEfficiency) {
                    speedEfficiency = speedEfficiency * 0.7
                }
            } else if (speed < 1000) {
                speedEfficiency = speedEfficiency * 1.3
            }

            // But the speed shoudn't rise too much too quickly, since it would make the convergence drop dramatically.
            val maxRise = 0.5   // Max rise: 50%
            speed = speed + Math.min(targetSpeed - speed, maxRise * speed)

            // Apply forces
            for (i <- graph.vertices.indices) {
                // Adaptive auto-speed: the speed of each node is lowered
                // when the node swings.
                val swinging = graph.vertices(i).mass * (nodesOldDx(i) - nodesDx(i)).length
                val factor = speed / (1 + Math.sqrt(speed * swinging))
                graph.vertices(i).pos = graph.vertices(i).pos + (nodesDx(i) * factor)
            }

            val t1 = System.currentTimeMillis()
            println(s"Iteration ${iteration + 1}/$iterations, ${t1 - t0} ms")
        }

        Pajek.dump(graph.map(n => n.pos).toImmutable, outFilePath)

    }
}