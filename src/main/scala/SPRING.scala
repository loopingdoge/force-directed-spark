/* 
    Implementation of 1984 algorithm by Eades 
    Eades, Peter. A heuristic for graph drawing Stanton. Ralph G., editor. Congressus numerantium. Winnipeg: Utilitas Mathematica; 1984 May: 149-160. ISBN: 0-919628-42-7.
*/

object SPRING {
    // original uses c1 = 2, c2 = 1, c3 = 1, c4 = 0.1, and M = 100
    val (c1, c2, c3, c4, maxIter) = (2.0, 1.0, 1.0, 0.1, 100)
    val (width, length) = (350, 350)

    def attractiveForce(d: Double) = c1 * math.log(d / c2)
    def repulsiveForce(d: Double) = c3 / (math.sqrt(d) + 0.0001)
    def attractiveForce(d: Vec2) = new Point2(math.log(d.x / c2), math.log(d.y / c2)) * c1
    def repulsiveForce(d: Vec2) = new Point2(1.0/(math.sqrt(d.x) + 0.0001), 1.0/(math.sqrt(d.y) + 0.0001)) * c3

    def layoutGraph(iteration: Int, allPairs: List[(Int, Int)], edges: List[(Int, Int)], vertices: List[Point2]): List[Point2] = {
        if (iteration == 0) {
            vertices
        } else {
            println(s"$iteration")
            layoutGraph(iteration - 1, allPairs, edges, (allPairs map {
                case (v, u) => (v, u, (vertices(v) - vertices(u)).abs())
            } collect {
                case (v, u, d) if edges contains (v, u) => (v, u, attractiveForce(d) * (-c4), attractiveForce(d) * c4)
                case (v, u, d) => (v, u, repulsiveForce(d) * c4, repulsiveForce(d) * (-c4))
            } flatMap {
                case (v: Int, u: Int, f1: Point2, f2: Point2) => List((v, vertices(v).shift(f1.x, f1.y)), (u, vertices(u).shift(f2.x, f2.y)))
            } groupBy(el => el._1) map {
                case (key, value) => key -> (value map(_._2) reduce (_ + _))
            }).toList.sortBy(_._1) map {
                case (id, pos) => pos
            })
        }
    }

    def main(args: Array[String]) {
        val p = Pajek
        val filePath = args(0)    // path to pajek net formatted input file
        val graph = p.parse(filePath)
        val vertexNum = graph.vertices.size
        
        //val area = frame.area
        //val optimalPairwiseDistance = math.sqrt(area / vertexNum)

        val vertices = (0 until vertexNum) map (_ => new Point2()) toList
        val allPairs = (for(x <- 0 until vertexNum; y <- (x + 1) until vertexNum if x != y) yield (x, y)) toList

        println(maxIter)
        val layoutedVertices = layoutGraph(maxIter, allPairs, graph.edges, vertices)
        val maxX = (layoutedVertices map(_.x)).max
        val maxY = (layoutedVertices map(_.y)).max
        val layoutedNorm = layoutedVertices map (p => new Point2(p.x/maxX * width, p.y/maxY * length))
        println("vertices")
        vertices.foreach(v => println(v))
        println
        println("layouted vertices")
        layoutedNorm.foreach(v => println(v))
        println
        Pajek.dump(new Graph(layoutedNorm, graph.edges), "out/pajekkone.net")

        // Old mutable SPRING
        /*
        var vertices = (0 to vertexNum) map (_ => new Point2()) toList
        for (_ <- 0 to maxIter) {
            for (
                u <- 0 to vertexNum;
                v <- (u + 1) to vertexNum if (u != v)
            ) {
                val distance = (vertices(v) - vertices(u)).abs()
                if (edges contains (v, u)) {
                    // attractiveForce force between v and u
                    // if you have a vector (v - u) (i.e. u -> v) then the attractiveForce force adds to u and substracts to v ???
                    val attractive = attractiveForce(distance * c4)
                    vertices(v) = vertices(v).shift(-attractive.x, -attractive.y) 
                    vertices(u) = vertices(u).shift(attractive.x, attractive.y)
                } else {
                    // repulsiveForce force
                    // if you have a vector (v - u) (i.e. u -> v) then the repulsiveForce force adds to v and substracts to u ???
                    val repulsive = repulsiveForce(distance * c4)
                    vertices(v) = vertices(v).shift(repulsive.x, repulsive.y)
                    vertices(u) = vertices(u).shift(-repulsive.x, -repulsive.y)
                }
            }
        }
        */
    }
}