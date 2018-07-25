/* A parser for Pajek graph definition format */

import scala.io.Source

object PajekParser {
    def parse(fileName: String): Graph[Int] = {
        var lines = Source.fromFile(fileName).getLines
        val nVertices = lines.next.split(" ")(1).toInt
        val vertices = (0 until nVertices).toList
        lines.next // skip *Edges line
        val edges = (lines.map(line => line.split(" ")).map(splitted => Tuple2(splitted(0).toInt, splitted(1).toInt))).toList
        new Graph(vertices, edges)
    }
}