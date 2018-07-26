/* 
    Implementation of 1984 algorithm by Eades 
    Eades, Peter. A heuristic for graph drawing Stanton. Ralph G., editor. Congressus numerantium. Winnipeg: Utilitas Mathematica; 1984 May: 149-160. ISBN: 0-919628-42-7.
*/
import scala.util.Random

object SPRING {
    val (c1, c2, c3, c4, maxIter) = (2.0, 1.0, 1.0, 0.1, 100)
    // original uses c1 = 2, c2 = 1, c3 = 1, c4 = 0.1, and M = 100

    def attractive(d: Double) = c1 * math.log(d / c2)
    def repulsive(d: Double) = c3 / (math.sqrt(d) + 0.0001)
    def attractive(d: Vec2) = new Point2(math.log(d.x / c2), math.log(d.y / c2)) * c1
    def repulsive(d: Vec2) = new Point2(1.0/(math.sqrt(d.x) + 0.0001), 1.0/(math.sqrt(d.y) + 0.0001)) * c3

    def layoutGraph(iteration: Int, allPairs: List[(Int, Int)], edges: List[(Int, Int)], vertices: List[Point2]): List[Point2] = {
        if (iteration == 0) {
            return vertices
        } else {
            return layoutGraph(iteration - 1, allPairs, edges, (allPairs map {
                case (v, u) => (v, u, (vertices(v) - vertices(u)).abs())
            } collect {
                case (v, u, d) if edges contains (v, u) => (v, u, attractive(d) * (-c4), attractive(d) * c4)
                case (v, u, d) => (v, u, repulsive(d) * c4, repulsive(d) * (-c4))
            } flatMap {
                case (v: Int, u: Int, f1: Point2, f2: Point2) => List((v, vertices(v).shift(f1.x, f1.y)), (u, vertices(u).shift(f2.x, f2.y)))
            } groupBy(el => el._1) map {
                case (key, value) => key -> (value.map(_._2) reduce (_ + _))
            }).toList.sortBy(_._1) map {
                case (id, pos) => pos
            })
        }
    }

    def main(args: Array[String]) {
        val p = PajekParser
        val filePath = args(0)    // path to pajek net formatted input file
        val graph = p.parse(filePath)
        val vertexNum = graph.vertices.size
        val edges = graph.edges
        
        val frame = new Point2(20, 20)
        //val area = frame.area
        //val optimalPairwiseDistance = math.sqrt(area / vertexNum)

        val vertices = (0 until vertexNum) map (_ => new Point2()) toList
        val allPairs = (for(x <- (0 until vertexNum); y <- ((x + 1) until vertexNum) if (x != y)) yield (x, y)) toList

        val layoutedVertices = layoutGraph(1, allPairs, edges, vertices)
        println("vertices")
        vertices.foreach(v => println(v))
        println
        println("layouted vertices")
        layoutedVertices.foreach(v => println(v))
        println

        // Old SPRING
        /*
        var vertices = (0 to vertexNum) map (_ => new Point2()) toList
        for (_ <- 0 to maxIter) {
            for (
                u <- 0 to vertexNum;
                v <- (u + 1) to vertexNum if (u != v)
            ) {
                val distance = (vertices(v) - vertices(u)).abs()
                if (edges contains (v, u)) {
                    // attractive force between v and u
                    // if you have a vector (v - u) (i.e. u -> v) then the attractive force adds to u and substracts to v ???
                    val attractiveForce = attractive(distance * c4)
                    vertices(v) = vertices(v).shift(-attractiveForce.x, -attractiveForce.y) 
                    vertices(u) = vertices(u).shift(attractiveForce.x, attractiveForce.y)
                } else {
                    // repulsive force
                    // if you have a vector (v - u) (i.e. u -> v) then the repulsive force adds to v and substracts to u ???
                    val repulsiveForce = repulsive(distance * c4)
                    vertices(v) = vertices(v).shift(repulsiveForce.x, repulsiveForce.y)
                    vertices(u) = vertices(u).shift(-repulsiveForce.x, -repulsiveForce.y)
                }
            }
        }
        */

        // Input G: Graph
        // Output: Drawing of G
        // Init: place vertices of G in random locations
        // for i to Iter do
        // |    compute force acting on each vertex as 
        // |        - adjacent vertices: c1 * log(d/c2) where d is length of the spring between two linked vertices, c1 and c2 are constants
        // |        - non-adjacent vertices: c3 / sqrt(d) where d is the distance between vertices
        // |_   move each vertex by c4 * force
    }
}