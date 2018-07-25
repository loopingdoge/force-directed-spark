class Graph[T] (val vertices: List[T], val edges: List[(Int, Int)]) {
    def map[U](f: T => U): Graph[U] = new Graph(vertices.map(f), edges)
}