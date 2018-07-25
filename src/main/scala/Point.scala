class Point2D(var x: Double, var y: Double) {
    def +(other: Point2D) = new Point2D(x + other.x, y + other.y)
    def -(other: Point2D) = new Point2D(x - other.x, y - other.y)
    // dot product
    def **(other: Point2D) = (x * other.x) + (y * other.y)

    // shift and scale jointly
    def shift(a: Double) = new Point2D(x + a, y + a)
    def scale(a: Double) = new Point2D(x * a, y * a)
    // shift and scale independently
    def shift(a: Double, b: Double) = new Point2D(x + a, y + b)
    def scale(a: Double, b: Double) = new Point2D(x * a, y * b)

    def area() = x * y
    override def toString() = s"($x, $y)"
}

class Point3D(var x: Double, var y: Double, var z: Double) {
    def +(other: Point3D) = new Point3D(x + other.x, y + other.y, z + other.z)
    def -(other: Point3D) = new Point3D(x - other.x, y - other.y, z - other.z)
    // dot product
    def **(other: Point3D) = (x * other.x) + (y * other.y) + (z * other.z)
 
    // shift and scale jointly
    def shift(a: Double) = new Point3D(x + a, y + a, z + a)
    def scale(a: Double) = new Point3D(x * a, y * a, z * a)
    // shift and scale independently
    def shift(a: Double, b: Double, c: Double) = new Point3D(x + a, y + b, z + c)
    def scale(a: Double, b: Double, c: Double) = new Point3D(x * a, y * b, z * c)

    def volume() = x * y * z
    override def toString() = s"($x, $y, $z)"
}