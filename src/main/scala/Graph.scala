import org.apache.spark.graphx.{Graph => XGraph, Edge}

class Graph[T] (val vertices: List[T], val edges: List[(Int, Int)]) {
    def map[U](f: T => U): Graph[U] = new Graph(vertices.map(f), edges)
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