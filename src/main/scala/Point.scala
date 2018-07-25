trait Number[T] {
    def +(other: T): T = this + other
    def -(other: T): T = this - other
    def *(other: T): T = this * other
    def /(other: T): T = this / other
}

class Point2D[T <: Number[T]](var x: T, var y: T) {
    def +(other: Point2D[T]) = new Point2D(x + other.x, y + other.y)
    def -(other: Point2D[T]) = new Point2D(x - other.x, y - other.y)
    // dot product
    def **(other: Point3D[T]) = (x * other.x) + (y * other.y)

    // shift and scale jointly
    def shift(a: T) = new Point2D(x + a, y + a)
    def scale(a: T) = new Point2D(x * a, y * a)
    // shift and scale independently
    def shift(other: Point2D[T]) = new Point2D(x + other.x, y + other.y)
    def scale(other: Point2D[T]) = new Point2D(x * other.x, y * other.y)

    def area() = x * y
    override def toString() = s"($x, $y)"
}

class Point3D[T <: Number[T]](var x: T, var y: T, var z: T) {
    def +(other: Point3D[T]) = new Point3D(x + other.x, y + other.y, z + other.z)
    def -(other: Point3D[T]) = new Point3D(x - other.x, y - other.y, z - other.z)
    // dot product
    def **(other: Point3D[T]) = (x * other.x) + (y * other.y) + (z * other.z)
 
    // shift and scale jointly
    def shift(a: T) = new Point3D(x + a, y + a, z + a)
    def scale(a: T) = new Point3D(x * a, y * a, z * a)
    // shift and scale independently
    def shift(other: Point3D[T]) = new Point3D(x + other.x, y + other.y, z + other.z)
    def scale(other: Point3D[T]) = new Point3D(x * other.x, y * other.y, z * other.z)

    def volume() = x * y * z
    override def toString() = s"($x, $y, $z)"
}