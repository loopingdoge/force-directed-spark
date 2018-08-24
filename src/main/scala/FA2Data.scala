trait FA2Data {

    val directed = false
    val barnesHutTheta = 1.2
    val edgeWeightInfluence = 0
    val gravity = 1.0
    val jitterTolerance = 1
//    val scalingRatio = 2.0
    val adjustSizes = false
    val barnesHutOptimize = false
    val linLogMode = false
    val strongGravityMode = false
    val outboundAttractionDistribution = true

    def repulsiveForce(v1: FANode, v2: FANode, coefficient: Double = 1): (Vec2, Vec2) = {
        val distanceVector = v1.pos - v2.pos
        val distanceLength: Double = distanceVector.length
        if (distanceLength > 0) {
            val factor: Double =
//                coefficient * v1.mass * v2.mass / distanceLength
                coefficient * v1.mass * v2.mass / distanceLength / distanceLength
            (distanceVector * factor, -distanceVector * factor)
        } else {
            (Vec2.zero, Vec2.zero)
        }
    }

    def attractiveForce(source: FANode, target: FANode, outboundAttractionCompensation: Double): (Vec2, Vec2) = {
        val distance = source.pos - target.pos
        val edgeWeight: Double = 1.0
        val factor = (linLogMode, outboundAttractionDistribution) match {
//            case (true, true) =>
//                -outboundAttractionCompensation * edgeWeight * Math.log(1 + distanceLength) / distanceLength / source.mass
//            case (true, false) =>
//                -outboundAttractionCompensation * edgeWeight * Math.log(1 + distanceLength) / distanceLength
            case (false, true) =>
                -outboundAttractionCompensation * edgeWeight / source.mass
            case (false, false) =>
                -outboundAttractionCompensation * edgeWeight
            case _ => throw new NotImplementedError("Linlog mode is not implemented yet")
        }
         (distance * factor, -distance * factor)
    }

    def gravityForce(v: FANode): Vec2 = {
        val distanceFromOriginVector = v.pos.toVec
        val distanceFromOriginLength: Double = v.pos.toVec.length
        if (distanceFromOriginLength > 0) {
            val factor: Double =
                if (strongGravityMode)
                    v.mass * gravity
                else
                    v.mass * gravity / distanceFromOriginLength
            -distanceFromOriginVector * factor
        } else {
            Vec2.zero
        }
    }

}