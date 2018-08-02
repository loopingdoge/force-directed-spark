import org.apache.spark.graphx.{Edge, Graph => XGraph}

import scala.reflect.ClassTag

class Graph[T] (val vertices: List[T], val edges: List[(Int, Int)]) {
    def map[U](f: T => U): Graph[U] = new Graph(vertices.map(f), edges)
    def map[U](f: (T, Int) => U): Graph[U] = new Graph(vertices.zipWithIndex.map { case (v, i) => f(v, i) }, edges)
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

class MutableGraph[T] (var vertices: Array[T], var edges: Array[(Int, Int)]) {
    def map[U: ClassTag](f: T => U): MutableGraph[U] = new MutableGraph(vertices.map(f).toArray, edges)
    def map[U: ClassTag](f: (T, Int) => U): MutableGraph[U] = new MutableGraph(vertices.zipWithIndex.map { case (v, i) => f(v, i) }, edges)
    def toImmutable: Graph[T] =
        new Graph(vertices.toList, edges.toList)
}

object MutableGraph {
    def fromImmutable[T: ClassTag](g: Graph[T]): MutableGraph[T] =
        new MutableGraph(g.vertices.toArray, g.edges.toArray)
}