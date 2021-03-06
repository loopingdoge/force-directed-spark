/* A parser for Pajek graph definition format */

import scala.io.Source
import java.io._

import org.apache.hadoop.fs.{FileSystem, Path}

object Parser {

    def parse(fs: FileSystem, fileName: String): ImmutableGraph[Int] = {
        val stream = fs.open(new Path(fileName))
        val lines = Source.fromInputStream(stream).getLines

        if( fileName.endsWith(".net") ) {
            Pajek.parse(lines)
        } else {
            SNAP.parse(lines)
        }
    }
}

object Pajek {

    def parse(lines: Iterator[String]): ImmutableGraph[Int] = {
        val nVertices = lines.next.split(" ")(1).toInt
        val vertices = (0 until nVertices).toVector

        if(lines.next != "*Edges") vertices.foreach(_ => lines.next)
        val edges = lines
            .map(line => line.split(" "))
            .map(split => (split(0).toInt - 1, split(1).toInt - 1))
            .toVector
        new ImmutableGraph(vertices, edges)
    }

    def dump(graph: ImmutableGraph[Point2], fs: FileSystem, filepath: String): Unit = {

        def round(num: Double): Double = {
            BigDecimal(num).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        }

        val file = fs.create(new Path(filepath))
        val bw = new PrintWriter(file)

        val verticesHeading = Vector(s"*Vertices ${graph.vertices.length}")
        val verticesPajek = graph
            .vertices
            .zipWithIndex
            .map { case (v, i) =>
                s"${i + 1} ${round(v.x)} ${round(v.y)}"
            }

        val edgesHeading = Vector("*Edges")
        val edgesPajek = graph
            .edges
            .map {
                case (u, v) =>
                    s"${u + 1} ${v + 1}"
            }

        val output = Vector(verticesHeading, verticesPajek, edgesHeading, edgesPajek)
            .flatten
            .mkString("\n")

        bw.write(output)
        bw.close()
    }

}

object SNAP {

    def parse(l: Iterator[String]): ImmutableGraph[Int] = {
        val lines = l
            .dropWhile( line => line.startsWith("#"))

        val edges = lines
            .map(line => line.split("\\s+"))
            // Index correction 0 -> 1
            .map(split => (split(0).toInt, split(1).toInt))
            .toVector

        val nVertices = 1 + edges
            .foldLeft[Int](0)( (maxId, edge: (Int, Int)) 
                => Math.max(maxId, Math.max(edge._1, edge._2)))

        val vertices = (1 to nVertices).toVector
        
        new ImmutableGraph(vertices, edges)
    }
}