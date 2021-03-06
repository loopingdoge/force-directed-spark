import org.apache.spark.graphx.{Edge, Graph => XGraph}

import scala.reflect.ClassTag

sealed class Graph[T]

case class ImmutableGraph[T] (vertices: Vector[T], edges: Vector[(Int, Int)]) extends Graph[T] {
    def map[U](f: T => U): ImmutableGraph[U] = new ImmutableGraph(vertices.map(f), edges)
    def map[U](f: (T, Int) => U): ImmutableGraph[U] = new ImmutableGraph(vertices.zipWithIndex.map { case (v, i) => f(v, i) }, edges)
    def incidents(vIndex: Int): Vector[Int] = {
        edges
            .filter( (e: (Int, Int)) => e._1 == vIndex || e._2 == vIndex )
            .map( (e: (Int, Int)) => if (e._1 == vIndex) e._2 else e._1 )
            .distinct
    }
}

object ImmutableGraph {
    def fromSpark[T, U](g: XGraph[T, U]): ImmutableGraph[T] = {
        val vertices = g.vertices
            .sortBy {
                case (id, _) => id
            }
            .collect
            .map {
                case (_, v) => v
            }
            .toVector
        val edges = g.edges
            .collect
            .map {
                case Edge(s, d, _) =>
                    (s.toInt, d.toInt)
            }
            .toVector
        new ImmutableGraph(vertices, edges)
    }
}

case class MutableGraph[T] (var vertices: Array[T], var edges: Array[(Int, Int)]) extends Graph[T] {
    def map[U: ClassTag](f: T => U): MutableGraph[U] = new MutableGraph(vertices.map(f), edges)
    def map[U: ClassTag](f: (T, Int) => U): MutableGraph[U] = new MutableGraph(vertices.zipWithIndex.map { case (v, i) => f(v, i) }, edges)
    def toImmutable: ImmutableGraph[T] =
        new ImmutableGraph(vertices.toVector, edges.toVector)
}

object MutableGraph {
    def fromImmutable[T: ClassTag](g: ImmutableGraph[T]): MutableGraph[T] =
        new MutableGraph(g.vertices.toArray, g.edges.toArray)
}

case class SparkGraph[T] (graph: XGraph[T, Null]) extends Graph[T]

