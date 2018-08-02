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
//    val scalingRatio = 2.0
    val adjustSizes = false
    val barnesHutOptimize = false
    val linLogMode = false
    val strongGravityMode = false
    val outboundAttractionDistribution = true

    def repulsiveForce(v1: FANode, v2: FANode, coefficient: Double = 1): (Vec2, Vec2) = {
        val distanceVector = v1.pos - v2.pos
        val distanceLength: Double = distanceVector.length
        if (distanceLength > 0) {
            val factor: Double =
//                coefficient * v1.mass * v2.mass / distanceLength
                coefficient * v1.mass * v2.mass / distanceLength / distanceLength
            (distanceVector * factor, -distanceVector * factor)
        } else {
            (Vec2.zero, Vec2.zero)
        }
    }

    def attractiveForce(source: FANode, target: FANode, outboundAttractionCompensation: Double): (Vec2, Vec2) = {
        val distance = source.pos - target.pos
        val edgeWeight: Double = 1.0
        val factor = (linLogMode, outboundAttractionDistribution) match {
//            case (true, true) =>
//                -outboundAttractionCompensation * edgeWeight * Math.log(1 + distanceLength) / distanceLength / source.mass
//            case (true, false) =>
//                -outboundAttractionCompensation * edgeWeight * Math.log(1 + distanceLength) / distanceLength
            case (false, true) =>
                -outboundAttractionCompensation * edgeWeight / source.mass
            case (false, false) =>
                -outboundAttractionCompensation * edgeWeight
            case _ => throw new NotImplementedError("Linlog mode is not implemented yet")
        }
        (distance * factor, -distance * factor)
    }

    def gravityForce(v: FANode): Vec2 = {
        val distanceFromOriginVector = v.pos.toVec
        val distanceFromOriginLength: Double = v.pos.toVec.length
        if (distanceFromOriginLength > 0) {
            val factor: Double =
                if (strongGravityMode)
                    v.mass * gravity
                else
                    v.mass * gravity / distanceFromOriginLength
            -distanceFromOriginVector * factor
        } else {
            Vec2.zero
        }
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

        val outboundAttractionCompensation =
            if (outboundAttractionDistribution) {
                graph.vertices.map(v => v.mass).sum / graph.vertices.length
            } else {
                1.0
            }

        for (iteration <- 0 until iterations) {
            val t0 = System.currentTimeMillis()

            for (i <- graph.vertices.indices) {
                nodesOldDx(i) = nodesDx(i)
                nodesDx(i) = Vec2.zero
            }

            for (
                i <- graph.vertices.indices;
                j <- i + 1 until graph.vertices.length
            ) {
                val (d1, d2) = repulsiveForce(graph.vertices(i), graph.vertices(j))
                nodesDx(i) = nodesDx(i) + d1
                nodesDx(j) = nodesDx(j) + d2
            }

            for (i <- graph.vertices.indices) {
                val displacement = gravityForce(graph.vertices(i))
                nodesDx(i) = nodesDx(i) + displacement
            }

            graph.edges.foreach {
                case (index1, index2) =>
                    val (d1, d2) = attractiveForce(graph.vertices(index1 - 1), graph.vertices(index2 - 1), outboundAttractionCompensation)
                    nodesDx(index1 - 1) = nodesDx(index1 - 1) + d1
                    nodesDx(index2 - 1) = nodesDx(index2 - 1) + d2
            }

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

            // Speed efficiency is how the speed really corresponds to the swinging vs. convergence tradeoff
            // We adjust it slowly and carefully
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
                val factor = speed / (1.0 + Math.sqrt(speed * swinging))
                graph.vertices(i).pos = graph.vertices(i).pos + (nodesDx(i) * factor)
            }

            val t1 = System.currentTimeMillis()
            println(s"Iteration ${iteration + 1}/$iterations, ${t1 - t0} ms")
        }

        Pajek.dump(graph.map(n => n.pos).toImmutable, outFilePath)

    }
}