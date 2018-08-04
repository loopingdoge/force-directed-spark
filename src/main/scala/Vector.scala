@SerialVersionUID(100L)
class Vec2(val x: Double, val y: Double) extends Serializable {
    def length: Double =
        Math.sqrt(Math.pow(x, 2) + Math.pow(y, 2))
    
    def normalize: Vec2 = {
        val l = this.length
        if (l != 0) {
            new Vec2(x / l, y / l)
        } else {
            Vec2.zero
        }
    }

    def *(s: Double): Vec2 =
        new Vec2(x * s, y * s)
    
    def +(s: Vec2): Vec2 =
        new Vec2(x + s.x, y + s.y)

    def -(s: Vec2): Vec2 =
        new Vec2(x - s.x, y - s.y)

    def -(s: Double): Vec2 =
        new Vec2(x - s, y - s)

    def /(s: Double): Vec2 =
        new Vec2(x / s, y / s)

    def unary_- =
        new Vec2(-x, -y)

    def abs() = 
        new Vec2(math.abs(x), math.abs(y))

    def toPoint =
        new Point2(x, y)

    override def toString: String = s"($x, $y)"
}

object Vec2 {
    def zero: Vec2 =
        new Vec2(0, 0)

    def apply(x: Double, y: Double): Vec2 =
        new Vec2(x, y)
}

@SerialVersionUID(100L)
class Vec3(val x: Double, val y: Double, val z: Double) extends Serializable {
    def length: Double =
        Math.sqrt(Math.pow(x, 2) + Math.pow(y, 2) + Math.pow(z, 2))

    def normalize: Vec3 = {
        val l = this.length
        if (l != 0) {
            new Vec3(x / l, y / l, z / l)
        } else {
            Vec3.zero
        }
    }

    def *(s: Double): Vec3 =
        new Vec3(x * s, y * s, z * s)

    def +(s: Vec3): Vec3 =
        new Vec3(x + s.x, y + s.y, z + s.z)

    def -(s: Vec3): Vec3 =
        new Vec3(x - s.x, y - s.y, z - s.z)

    def /(s: Double): Vec3 =
        new Vec3(x / s, y / s, z / s)

    def abs() = 
        new Vec3(math.abs(x), math.abs(y), math.abs(z))

    def toPoint =
        new Point3(x, y, z)

    override def toString: String = s"($x, $y, $z)"
}

object Vec3 {
    def zero: Vec3 =
        new Vec3(0, 0, 0)

    def apply(x: Double, y: Double, z: Double): Vec3 =
        new Vec3(x, y, z)
}