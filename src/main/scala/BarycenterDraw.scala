import java.awt.geom.Point2D

object BaricenterDraw {

//    def findPolygon(graph: Graph[Int]): List[Int] = {
//        // Prendo 2 nodi collegati e faccio partire una
//    }

     def run() {
         val graph = Pajek.parse("data/3planar-17352v.net")

         val incidents: List[List[Int]] = (0 until graph.vertices.length) map (v => graph.incidents( v + 1 )) toList

         print(incidents)

         // TODO Initialize V0: place fixed vertices u ∈ V0 at corners of P;

         // Initialize V1: place free vertices v ∈ V1 at the origin;
         var vPositions = List.fill(graph.vertices.length)(new Point2(0.0, 0.0))

         vPositions = vPositions.zipWithIndex.map{
             case (vPosition, i) => {
                 if(i == 1) new Point2(100000.0, 200000.0) else
                 if(i == 34) new Point2(175000.0, 150000.0) else
                 if(i == 35) new Point2(100000.0, 100000.0) else
                     vPosition
             }}

         println(vPositions)

         for (x <- 1 to 1000) {
             println(s"Iteration ${x}/10")
             // foreach free vertex v ∈ V1 do
            vPositions = vPositions.zipWithIndex.map {
                 case (vPosition, i) =>
                     if(i == 1) new Point2(100000.0, 20000.0) else
                     if(i == 34) new Point2(175000.0, 150000.0) else
                     if(i == 35) new Point2(100000.0, 10000.0) else
                         new Point2(
                             1.0/incidents(i).length * incidents(i).foldLeft[Double](0.0)( (xCoord, a) => xCoord + vPositions(a-1).x ),
                             1.0/incidents(i).length * incidents(i).foldLeft[Double](0.0)( (yCoord, a) => yCoord + vPositions(a-1).y )
                         )
             }
         }
         println(vPositions)

         Pajek.dump(new Graph(vPositions, graph.edges), "out/3planar-17352v.net")

     }
 }

// Input: G = (V, E) with V = V0 ∩ V1, with fixed vertices V0 and free vertices V1; a strictly
// convex polygon P with |V0| vertices.
// Output: a position pv for each vertex of V , such that the fixed vertices form a convex
// polygon P.
//
//
//
// repeat
// foreach free vertex v ∈ V1 do
// xv ← 1
// deg(v)
// P
// (u,v)∈E
// xu;
// yv ← 1
// deg(v)
// P
// (u,v)∈E
// yu;
// until xv and yv converge for all free vertices v;