/* A parser for Pajek graph definition format */

import scala.io.Source
import org.apache.spark.graphx.{Graph => XGraph, Edge, VertexId}
import java.io._

object Pajek {
    def parse(fileName: String): Graph[Int] = {
        var lines = Source.fromFile(fileName).getLines
        val nVertices = lines.next.split(" ")(1).toInt
        val vertices = (0 until nVertices).toList
        lines.next // skip *Edges line
        val edges = (lines.map(line => line.split(" ")).map(splitted => Tuple2(splitted(0).toInt, splitted(1).toInt))).toList
        new Graph(vertices, edges)
    }

    def dump(graph: Graph[Point2], filepath: String) = {
        val file = new File(filepath)
        val bw = new BufferedWriter(new FileWriter(file))

        var output = s"*Vertices ${graph.vertices.length}\n"
        graph
            .vertices
            .zipWithIndex
            .foreach { case (v, i) => 
                output += s"${i + 1} ${v.x} ${v.y}\n"
            }

        output += "*Edges\n"
        graph
            .edges
            .foreach {
                case (u, v) =>
                    output += s"${u} ${v}\n"
            }

        bw.write(output)
        bw.close()
    }

}