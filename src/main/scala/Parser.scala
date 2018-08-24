/* A parser for Pajek graph definition format */

import scala.io.Source
import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Parser {
    val conf = new Configuration()
    val fs: FileSystem = FileSystem.get(conf)

    def parse(fileName: String): ImmutableGraph[Int] = {
        val source = Source.fromFile(fileName)

        val lines = source.getLines
        if( lines.next.startsWith("*Vertices") ) {
            Pajek.parse(source.reset)
        } else {
            SNAP.parse(source.reset)
        }
    }
}

object Pajek {
    val conf = new Configuration()
//    val fs: FileSystem = new Path("gs://force-directed-bucket").getFileSystem(conf)
    val fs: FileSystem = FileSystem.get(conf)

    def parse(source: Source): ImmutableGraph[Int] = {
        val lines = source.getLines
        val nVertices = lines.next.split(" ")(1).toInt
        val vertices = (0 until nVertices).toVector

        if(lines.next != "*Edges") vertices.foreach(_ => lines.next)
        val edges = lines
            .map(line => line.split(" "))
            .map(split => (split(0).toInt - 1, split(1).toInt - 1))
            .toVector
        new ImmutableGraph(vertices, edges)
    }

    def dump(graph: ImmutableGraph[Point2], filepath: String): Unit = {

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

    def parse(source: Source): ImmutableGraph[Int] = {
        val lines = source
            .getLines
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
        print(s"$edges \n$nVertices \n $vertices \n\n")
        
        new ImmutableGraph(vertices, edges)
    }
}