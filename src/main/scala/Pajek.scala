/* A parser for Pajek graph definition format */

import scala.io.Source
import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Pajek {

    val conf = new Configuration()
//    val fs: FileSystem = new Path("gs://force-directed-bucket").getFileSystem(conf)
    val fs: FileSystem = FileSystem.get(conf)

    def parse(fileName: String): Graph[Int] = {
        val stream = fs.open(new Path(fileName))
        val lines = Source.fromInputStream(stream).getLines
        val nVertices = lines.next.split(" ")(1).toInt
        val vertices = (0 until nVertices).toList
        vertices.foreach(_ => lines.next)
        lines.next // skip *Edges line
        val edges = lines.map(line => line.split(" ")).map(splitted => Tuple2(splitted(0).toInt, splitted(1).toInt)).toList
        new Graph(vertices, edges)
    }

    def dump(graph: Graph[Point2], filepath: String): Unit = {

        def round(num: Double): Double = {
            BigDecimal(num).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        }

        val file = fs.create(new Path(filepath))
        val bw = new PrintWriter(file)

        val verticesHeading = List(s"*Vertices ${graph.vertices.length}")
        val verticesPajek = graph
            .vertices
            .zipWithIndex
            .map { case (v, i) =>
                s"${i + 1} ${round(v.x)} ${round(v.y)}"
            }

        val edgesHeading = List("*Edges")
        val edgesPajek = graph
            .edges
            .map {
                case (u, v) =>
                    s"$u $v"
            }

        val output = List(verticesHeading, verticesPajek, edgesHeading, edgesPajek)
            .flatten
            .mkString("\n")

        bw.write(output)
        bw.close()
    }

}