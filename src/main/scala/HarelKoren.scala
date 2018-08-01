object HarelKoren {
    val iterations = 4  // # iterations of local beautification
    val ratio = 3       // ratio of vertex sets in consecutive levels
    val rad = 7         // radius of local neighborhood
    val minSize = 10    // size of coarsest graph

    def run(args: Array[String]) = {
        /*  
            Compute all-pairs shortest path lengths d_{i, j}
            Init a layout L by placing vertices at random
            k <- minSize

            while k <= |V|
                C <- K-Centers(Graph, k)
                radius = max_{v \in C} min_{u \in C}{d_{u,v}} * rad
                LocalLayout(d_{CxC}, L(C), radius, iterations)
                foreach v \in V do
                    L(v) \in L(center(v)) + rand
                k <- k * ratio
        */
    }

    def kCenters(graph: Graph[Point2], k: Int) {
        // Compute set S \subset V s.t. |S| = k and (max_{v\in V}min_{s\in S}{d_{sv}}) is minimized
        /* 
            S <- {v} random
            for i = 2 to k do
                find u farthest from S (i.e. from all v in S) i.e.
                    \forall w in V . min_S{d_us} >= min_S{d_ws}
            S <- S unito {u}
            ret S
        */
    }

    def localLayout(layout: Graph[Point2], k: Int) {
        // Compute locally nice layout by beautifying k-neighborhoods
        /* 
            for i = 1 to iterations * |V| do
                let v be the vertex with max \Delta^k_v
                Compute \delta^k_v as in Kamada-Kawai
                L(v) <- L(v) + (\delta^k_v(x), \delta^k_v(y))
        */
    }
}