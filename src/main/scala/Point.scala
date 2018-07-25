class Point2D(var x: Float, var y: Float) {
    def +(other: Point2D) = new Point2D(x + other.x, y + other.y)
    def -(other: Point2D) = new Point2D(x - other.x, y - other.y)
    // dot product
    def **(other: Point2D) = (x * other.x) + (y * other.y)

    // shift and scale jointly
    def shift(a: Float) = new Point2D(x + a, y + a)
    def scale(a: Float) = new Point2D(x * a, y * a)
    // shift and scale independently
    def shift(a: Float, b: Float) = new Point2D(x + a, y + b)
    def scale(a: Float, b: Float) = new Point2D(x * a, y * b)

    def area() = x * y
    override def toString() = s"($x, $y)"
}

class Point3D(var x: Float, var y: Float, var z: Float) {
    def +(other: Point3D) = new Point3D(x + other.x, y + other.y, z + other.z)
    def -(other: Point3D) = new Point3D(x - other.x, y - other.y, z - other.z)
    // dot product
    def **(other: Point3D) = (x * other.x) + (y * other.y) + (z * other.z)
 
    // shift and scale jointly
    def shift(a: Float) = new Point3D(x + a, y + a, z + a)
    def scale(a: Float) = new Point3D(x * a, y * a, z * a)
    // shift and scale independently
    def shift(a: Float, b: Float, c: Float) = new Point3D(x + a, y + b, z + c)
    def scale(a: Float, b: Float, c: Float) = new Point3D(x * a, y * b, z * c)

    def volume() = x * y * z
    override def toString() = s"($x, $y, $z)"
}