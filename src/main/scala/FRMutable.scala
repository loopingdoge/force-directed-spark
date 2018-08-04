import org.apache.spark._

object FRMutable extends FRData with Layouter[MutableGraph] {

    class FRNode(var pos: Point2, var displacement: Vec2)

    private val area = width * length
    private var k: Double = 0.0
    private var iterations: Int = 0

    override def start(sc: SparkContext, inFilePath: String, iterations: Int): MutableGraph[Point2] = {
        // Place vertices at random
        val parsedGraph = Pajek.parse(inFilePath)
            .map { _ => new Point2(Math.random() * width, Math.random() * length) }

        // Create the mutable graph
        val initialGraph = MutableGraph.fromImmutable(parsedGraph)

        this.k = Math.sqrt(area / initialGraph.vertices.length) // Optimal pairwise distance
        this.iterations = iterations

        initialGraph
    }

    override def run(i: Int, g: MutableGraph[Point2]): MutableGraph[Point2] = {
        val (vertices, edges) =
            (
                g.vertices.map(pos => new FRNode(pos, Vec2.zero)),
                g.edges
            )

        val t = temperature(i, iterations)

        for (v <- vertices) {
            v.displacement = Vec2.zero
        }

        for (i <- vertices.indices; j <- vertices.indices) {
            if (i != j) {
                val (v1, v2) = (vertices(i), vertices(j))
                val delta = v1.pos - v2.pos
                val displacement = delta.normalize * repulsiveForce(k, delta.length)
                v1.displacement += displacement
            }
        }

        for (v <- vertices) {
            v.displacement += gravityForce(v.pos, k)
        }

        for ((v1Index, v2Index) <- edges) {
            val delta = vertices(v1Index).pos - vertices(v2Index).pos
            val displacement = delta.normalize * attractiveForce(k, delta.length)
            vertices(v1Index).displacement -= displacement
            vertices(v2Index).displacement += displacement
        }

        for (v <- vertices) {
            v.pos += v.displacement.normalize * Math.min(t, v.displacement.length)
        }

        new MutableGraph[Point2](vertices.map(v => v.pos), edges)
    }

    override def end(graph: MutableGraph[Point2], outFilePath: String): Unit = {
        Pajek.dump(graph.toImmutable, outFilePath)
    }
}