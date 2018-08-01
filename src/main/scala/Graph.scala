import org.apache.spark.graphx.{Graph => XGraph, Edge}

class Graph[T] (val vertices: List[T], val edges: List[(Int, Int)]) {
    def map[U](f: T => U): Graph[U] = new Graph(vertices.map(f), edges)
    def incidents[T](vIndex: Int): List[Int] = {
        edges
            .filter( (e: (Int, Int)) => e._1 == vIndex || e._2 == vIndex )
            .map( (e: (Int, Int)) => if (e._1 == vIndex) e._2 else e._1 )
            .distinct
    }
}

object Graph {
    def fromSpark[T, U](g: XGraph[T, U]): Graph[T] = {
        val vertices = g.vertices
            .collect
            .map {
                case (_, v) => v
            }
            .toList
        val edges = g.edges
            .collect
            .map {
                case Edge(s, d, _) =>
                    (s.toInt, d.toInt)
            }
            .toList
        new Graph(vertices, edges)
    }
}