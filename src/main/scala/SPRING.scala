/* 
    Implementation of 1984 algorithm by Eades 
    Eades, Peter. A heuristic for graph drawing Stanton. Ralph G., editor. Congressus numerantium. Winnipeg: Utilitas Mathematica; 1984 May: 149-160. ISBN: 0-919628-42-7.
*/
import scala.util.Random

class Vertex2D(id: Int = -1) {
    var pos = new Point2D(Random.nextDouble(), Random.nextDouble())
    override def toString() = s"$id: $pos"

    def -(other: Vertex2D) = this.pos - other.pos
}

object SPRING {
    val (c1, c2, c3, c4, maxIter) = (2, 1, 1, 0.4f, 100)

    def attractive(d: Double) = c1 * math.log(d / c2).toDouble
    def repulsive(d: Double) = c3 / math.sqrt(d).toDouble
    
    def main(args: Array[String]) {
        // original uses c1 = 2, c2 = 1, c3 = 1, c4 = 0.1, and M = 100
        val p = PajekParser
        val filePath = args(0)    // path to pajek net formatted input file
        val graph = p.parse(filePath)
        val vertexNum = graph.vertices.size
        val edges = graph.edges
        
        val frame = new Point2D(20, 20)
        val area = frame.area
        val optimalPairwiseDistance = math.sqrt(area / vertexNum)

        var vertices = ((0 to vertexNum) map (i => new Vertex2D(i))).toList
        
        val pairs = (for(x <- (0 to vertexNum); y <- (0 to vertexNum)) yield (x, y)).toSet.toList
        println(pairs)

        // TODO: scrivere codice effettivamente funzionante
        for (i <- 1 to 2) {
            // coppie di vertici non connessi aka forza repulsiva
            val repulsiveDispl = (pairs filter (p => !(edges contains p))).map({
                case (i, j) => vertices(i).pos - vertices(j).pos
            })
            // coppie di vertici connessi aka forza attrattiva
            val attractiveDispl = (pairs filter (p => edges contains p)).map({
                case (i, j) => vertices(i).pos - vertices(j).pos
            })
        }

        //vertices.foreach(v => println(v))

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