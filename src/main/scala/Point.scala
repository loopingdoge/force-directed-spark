import scala.util.Random

@SerialVersionUID(100L)
class Point2(var x: Double = Random.nextDouble, var y: Double = Random.nextDouble) extends Serializable {
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

    def abs() = new Point2(math.abs(x), math.abs(y))

    def area = x * y
    override def toString = s"($x, $y)"
}

object Point2 {
    def random() = new Point2
    def zero() = new Point2(0.0, 0.0)
    def one() = new Point2(1.0, 1.0)
}

@SerialVersionUID(100L)
class Point3(var x: Double = Random.nextDouble, var y: Double = Random.nextDouble, var z: Double = Random.nextDouble) extends Serializable {
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

    def abs() = new Point3(math.abs(x), math.abs(y), math.abs(z))

    def volume = x * y * z
    override def toString = s"($x, $y, $z)"
}

object Point3 {
    def random() = new Point3
}