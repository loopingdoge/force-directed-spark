trait FRData {

    val width = 1000
    val length = 1000
    val initialTemperature: Double = width / 10
    val gravity = 10

    def repulsiveForce(k: Double, x: Double): Double = {
        val divisor = if (x == 0) 0.001 else x
        Math.pow(k, 2) / divisor
    }

    def attractiveForce(k: Double, x: Double): Double = {
        val divisor = if (x == 0) 0.001 else x
        Math.pow(x, 2) / divisor
    }

    def gravityForce(pos: Point2, k: Double): Vec2 = {
        val distanceFromOriginLength: Double = pos.toVec.length
        if (distanceFromOriginLength > 0) {
            val factor = 0.01 * k * gravity * distanceFromOriginLength
            -(pos.toVec * factor) / distanceFromOriginLength
        } else {
            Vec2.zero
        }
    }

    // Inverse linear temperature decay
    def temperature(currIter: Int, maxIter: Int): Double = {
        -((currIter - maxIter).toDouble / maxIter) * initialTemperature
    }
}