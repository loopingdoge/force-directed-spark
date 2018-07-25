/* A parser for Pajek graph definition format */

import scala.io.Source

class PajekParser {
    def parse(fileName: String): Tuple2[Int, List[(Int, Int)]] = {
        var lines = Source.fromFile(fileName).getLines
        val vertices = lines.next.split(" ")(1).toInt
        lines.next // skip *Edges line
        val edges = (lines.map(line => line.split(" ")).map(splitted => Tuple2(splitted(0).toInt, splitted(1).toInt))).toList
        Tuple2(vertices, edges)
    }
}