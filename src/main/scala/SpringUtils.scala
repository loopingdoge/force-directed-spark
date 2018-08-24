object SpringUtils {
    // original uses c1 = 2, c2 = 1, c3 = 1, c4 = 0.1, and M = 100
    val (c1, c2, c3, c4) = (4.0, 1.0, 2.0, 0.01)
    val (width, length) = (1000, 1000)

    // Algorithm forces
    def attractiveForce(d: Double) = c1 * math.log((d + 0.0001) / c2)
    def repulsiveForce(d: Double) = c3 / (math.sqrt(d) + 0.0001)
}