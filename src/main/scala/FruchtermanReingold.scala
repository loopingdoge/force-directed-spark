class FRVert(x: Double, y: Double, val disp: Vec2) extends Point2(x, y) {
    override def toString = s"($x, $y, $disp)"
}

object FruchtermanReingold {
    def repulsiveForce(k: Double, x: Double): Double = {
        Math.pow(k, 2) / x
    }

    def attractiveForce(k: Double, x: Double): Double = {
        Math.pow(x, 2) / k
    }

    def run(iterations: Int) {
        // Frame: width W and length L
        val width = 800
        val length = 600
        val area = width * length

        // Place vertices at random
        val graph = PajekParser.parse("data/sample_graph.net")
            .map { _ => new FRVert(Math.random() * width, Math.random() * length, Vec2.zero) }

        // Optimal pairwise distance
        val k = Math.sqrt(area / graph.vertices.length)

        // TODO: cycle
        val graphWithRepulsion = graph.vertices.map { v =>
            var displacement = Vec2.zero
            for (u <- graph.vertices) {
                if (u != v) {
                    val delta = v - u
                    displacement = displacement + delta.normalize * repulsiveForce(k, delta.length)
                }
            }
            new FRVert(v.x, v.y, displacement)
        }

        // TODO: continue
    }
}