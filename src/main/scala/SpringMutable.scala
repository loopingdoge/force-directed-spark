/* 
    Implementation of 1984 algorithm by Eades 
    Eades, Peter. A heuristic for graph drawing Stanton. Ralph G., editor. Congressus numerantium. Winnipeg: Utilitas Mathematica; 1984 May: 149-160. ISBN: 0-919628-42-7.
*/

import org.apache.spark._
import org.apache.spark.graphx.{Graph => XGraph, Edge, VertexId}
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.rdd.RDD

object SpringMutable extends Layouter[Point2, ImmutableGraph] {
    val c4 = SpringUtils.c4
    val (width, length) = (SpringUtils.width, SpringUtils.length)
    var vertices: Array[(Double, Double)] = new Array(0)

    def start(sc: SparkContext, fs: FileSystem,  inFilePath: String, iterations: Int): ImmutableGraph[Point2] = {
        val parsedGraph = Parser.parse(fs, inFilePath)
            .map { _ => new Point2(Math.random, Math.random) }

        val vertexNum = parsedGraph.vertices.size

        vertices = new Array[(Double, Double)](vertexNum)
        for (i <- 0 until vertexNum) {
            vertices(i) = (parsedGraph.vertices(i).x, parsedGraph.vertices(i).y)
        }

        new ImmutableGraph[Point2](parsedGraph.vertices, parsedGraph.edges)
    }

    def run(iteration: Int, graph: ImmutableGraph[Point2]): ImmutableGraph[Point2] = {
        val (vertexNum, edges) = (graph.vertices.size, graph.edges)

        // Repulsive forces iteration
        for (
            v <- 0 until (vertexNum - 1);
            u <- (v + 1) until vertexNum
        ) {
            // repulsiveForce force
            // if you have a vector (v - u) (i.e. u -> v) then the repulsiveForce force adds to v and substracts to u ???
            val distance = (vertices(v)._1 - vertices(u)._1, vertices(v)._2 - vertices(u)._2)
            val length = Math.sqrt(Math.pow(distance._1, 2) + Math.pow(distance._2, 2))
            val normDistance = (distance._1 / length, distance._2 / length)

            val repulsive = SpringUtils.repulsiveForce(length) * c4

            val displacement = (normDistance._1 * repulsive, normDistance._2 * repulsive)
            vertices(v) = (vertices(v)._1 + displacement._1, vertices(v)._2 + displacement._2)
            vertices(u) = (vertices(u)._1 - displacement._1, vertices(u)._2 - displacement._2)
        }

        // Attractive forces iteration
        for ((v, u) <- edges) {
            val distance = (vertices(v)._1 - vertices(u)._1, vertices(v)._2 - vertices(u)._2)
            val length = Math.sqrt(Math.pow(distance._1, 2) + Math.pow(distance._2, 2))
            val normDistance = (distance._1 / length, distance._2 / length)

            val attractive = SpringUtils.attractiveForce(length) * c4

            val displacement = (normDistance._1 * attractive, normDistance._2 * attractive)
            vertices(v) = (vertices(v)._1 - displacement._1, vertices(v)._2 - displacement._2)
            vertices(u) = (vertices(u)._1 + displacement._1, vertices(u)._2 + displacement._2)
        }

        new ImmutableGraph((0 until vertexNum) map (i => new Point2(vertices(i)._1, vertices(i)._2)) toVector, edges)
    }

    def end(graph: ImmutableGraph[Point2], fs: FileSystem,  outFilePath: String) = {
        val (vertexNum, edges) = (graph.vertices.size, graph.edges)

        val layoutedVertices = (0 until vertexNum) map (i => new Point2(vertices(i)._1, vertices(i)._2)) toVector
        val (maxX, maxY) = ((layoutedVertices map(_.x)) max, (layoutedVertices map(_.y)) max)
        val layoutedNorm = layoutedVertices map (p => new Point2(p.x/maxX * width, p.y/maxY * length))
        Pajek.dump(new ImmutableGraph(layoutedNorm, edges), fs, outFilePath)
    }
}