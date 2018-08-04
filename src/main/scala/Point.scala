@SerialVersionUID(100L)
class Point2(val x: Double = Math.random, val y: Double = Math.random) extends Serializable {
    def +(other: Point2) = new Point2(x + other.x, y + other.y)
    def -(other: Point2) = new Vec2(x - other.x, y - other.y)

    def +(a: Vec2) = new Point2(x + a.x, y + a.y)
    // dot product
    def **(other: Point2) = (x * other.x) + (y * other.y)

    // shift and scale jointly
    def shift(a: Double) = new Point2(x + a, y + a)
    def +(a: Double) = new Point2(x + a, y + a)
    def scale(a: Double) = new Point2(x * a, y * a)
    def *(a: Double) = new Point2(x * a, y * a)
    // shift and scale independently
    def shift(a: Double, b: Double) = new Point2(x + a, y + b)
    def scale(a: Double, b: Double) = new Point2(x * a, y * b)

    def abs = new Point2(math.abs(x), math.abs(y))
    def round = new Point2(math.round(x), math.round(y))

    def area = x * y
    def toVec: Vec2 = new Vec2(x, y)
    override def toString = s"($x, $y)"
}

object Point2 {
    def random = new Point2(Math.random(), Math.random())
    def zero = new Point2(0.0, 0.0)
    def one = new Point2(1.0, 1.0)
    def apply(x: Double, y: Double) = new Point2(x, y)
}

@SerialVersionUID(100L)
class Point3(val x: Double = Math.random, val y: Double = Math.random, val z: Double = Math.random) extends Serializable {
    def +(other: Point3) = new Point3(x + other.x, y + other.y, z + other.z)
    def -(other: Point3) = new Vec3(x - other.x, y - other.y, z - other.z)

    def +(a: Vec3) = new Point3(x + a.x, y + a.y, z + a.z)
    // dot product
    def **(other: Point3) = (x * other.x) + (y * other.y) + (z * other.z)
 
    // shift and scale jointly
    def shift(a: Double) = new Point3(x + a, y + a, z + a)
    def +(a: Double) = new Point3(x + a, y + a, z + a)
    def scale(a: Double) = new Point3(x * a, y * a, z * a)
    def *(a: Double) = new Point3(x * a, y * a, z * a)
    // shift and scale independently
    def shift(a: Double, b: Double, c: Double) = new Point3(x + a, y + b, z + c)
    def scale(a: Double, b: Double, c: Double) = new Point3(x * a, y * b, z * c)

    def abs = new Point3(math.abs(x), math.abs(y), math.abs(z))
    def round = new Point3(math.round(x), math.round(y), math.round(z))

    def volume = x * y * z
    def toVec: Vec3 = new Vec3(x, y, z)
    override def toString = s"($x, $y, $z)"
}

object Point3 {
    def random = new Point3(Math.random, Math.random, Math.random)
    def zero = new Point3(0.0, 0.0, 0.0)
    def one = new Point3(1.0, 1.0, 1.0)
    def apply(x: Double, y: Double, z: Double) = new Point3(x, y, z)
}