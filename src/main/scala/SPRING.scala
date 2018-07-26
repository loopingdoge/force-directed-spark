/* 
    Implementation of 1984 algorithm by Eades 
    Eades, Peter. A heuristic for graph drawing Stanton. Ralph G., editor. Congressus numerantium. Winnipeg: Utilitas Mathematica; 1984 May: 149-160. ISBN: 0-919628-42-7.
*/
import scala.util.Random

class Vertex2D(id: Int = -1) {
    var pos = Point2D.random()
    var disp = 0.0

    def -(other: Vertex2D) = this.pos - other.pos
    
    override def toString() = s"$id: $pos"
}

object SPRING {
    val (c1, c2, c3, c4, maxIter) = (2.0, 1.0, 1.0, 0.4, 100)

    def attractive(d: Double) = c1 * math.log(d / c2)
    def repulsive(d: Double) = c3 / (math.sqrt(d) + 0.0001)
    def attractive(d: Vec2) = new Point2D(math.log(d.x / c2), math.log(d.y / c2)) * c1
    def repulsive(d: Vec2) = new Point2D(1.0/(math.sqrt(d.x) + 0.0001), 1.0/(math.sqrt(d.y) + 0.0001)) * c3
    
    def main(args: Array[String]) {
        // original uses c1 = 2, c2 = 1, c3 = 1, c4 = 0.1, and M = 100
        val p = PajekParser
        val filePath = args(0)    // path to pajek net formatted input file
        val graph = p.parse(filePath)
        val vertexNum = graph.vertices.size
        val edges = graph.edges
        
        val frame = new Point2D(20, 20)
        //val area = frame.area
        //val optimalPairwiseDistance = math.sqrt(area / vertexNum)

        var vertices = ((0 to vertexNum) map (i => new Vertex2D(i))).toList
        
        // all non duplicate & non identical pairs
        val pairs = (for(x <- (0 to vertexNum); y <- ((x + 1) to vertexNum) if (x != y)) yield (x, y))

        for (
            u <- 0 to vertexNum;
            v <- (u + 1) to vertexNum if (u != v)
        ) {
            val distance = (vertices(v) - vertices(u)).abs()
            if (edges contains (v, u)) {
                // attractive force between v and u
                // if you have a vector (v - u) (i.e. u -> v) then the attractive force adds to u and substracts to v ???
                val attractiveForce = attractive(distance * c4)
                vertices(v).pos = vertices(v).pos.shift(-attractiveForce.x, -attractiveForce.y) 
                vertices(u).pos = vertices(u).pos.shift(attractiveForce.x, attractiveForce.y)
            } else {
                // repulsive force
                // if you have a vector (v - u) (i.e. u -> v) then the repulsive force adds to v and substracts to u ???
                val repulsiveForce = repulsive(distance * c4)
                vertices(v).pos = vertices(v).pos.shift(repulsiveForce.x, repulsiveForce.y)
                vertices(u).pos = vertices(u).pos.shift(-repulsiveForce.x, -repulsiveForce.y)
            }
        }

        vertices.foreach(v => println(v))

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