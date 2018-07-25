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
        // Optimal pairwise distance
        val k = Math.sqrt(area / graph.vertices.length)
        // TODO: il resto
    }
}