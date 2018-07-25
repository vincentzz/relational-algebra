package me.vincentzz.relational.typedesc

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Base64

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

trait TypeDesc[+T] {
  val actualType: Class[_]

  def cast : Any    => T      = actualType.asInstanceOf[Class[T]].cast(_)

  def check: Any    => Unit   = o => assert(o.getClass == this.actualType, s"Type check Failed. Found: ${o.getClass.getSimpleName}, Expect: ${actualType.getSimpleName}")

  def show : Any    => String = cast(_).toString

  def read : String => T
}

object TypeDesc {
  def typeDescOf[T]: T => TypeDesc[_] = {
    case _: Int => INT
    case _: Long => LONG
    case _: Float => FLOAT
    case _: Char => CHAR
    case _: Byte => BYTE
    case _: Short => SHORT
    case _: Double => DOUBLE
    case _: String => STRING
    case _: DateTime => DATE
    case _: Any => OBJECT
  }
}

case object INT extends TypeDesc[Int] {
  override val actualType: Class[java.lang.Integer] = classOf[java.lang.Integer]

  override def toString = ":Int"

  override def read: String => Int = _.trim.toInt
}

case object LONG extends TypeDesc[Long] {
  override val actualType: Class[java.lang.Long] = classOf[java.lang.Long]

  override def toString = ":Long"

  override def read: String => Long = _.trim.toLong
}

case object FLOAT extends TypeDesc[Float] {
  override val actualType: Class[java.lang.Float] = classOf[java.lang.Float]

  override def toString = ":Float"

  override def read: String => Float = _.trim.toFloat
}

case object CHAR extends TypeDesc[Char] {
  override val actualType: Class[java.lang.Character] = classOf[java.lang.Character]

  override def toString = ":Char"

  override def read: String => Char = s => {
    assert(s.length == 1, s"cannot convert string $s to char")
    s.charAt(0)
  }
}

case object BYTE extends TypeDesc[Byte] {
  override val actualType: Class[java.lang.Byte] = classOf[java.lang.Byte]

  override def toString = ":Byte"

  override def read: String => Byte  = _.trim.toInt.toByte
}

case object SHORT extends TypeDesc[Short] {
  override val actualType: Class[java.lang.Short] = classOf[java.lang.Short]

  override def toString = ":Short"

  override def read: String => Short = _.trim.toShort
}

case object DOUBLE extends TypeDesc[Double] {
  override val actualType: Class[java.lang.Double] = classOf[java.lang.Double]

  override def toString = ":Double"

  override def read: String => Double = _.trim.toDouble
}

case object DATE extends TypeDesc[DateTime] {
  val fmt = "yyyy-MMM-dd HH:mm:ss.SSS ZZ"

  override val actualType: Class[DateTime] = classOf[DateTime]

  override def toString = ":Date"

  override def show: Any => String = cast(_).toString(fmt)

  override def read: String => DateTime = DateTime.parse(_, DateTimeFormat.forPattern(fmt))
}

case object STRING extends TypeDesc[String] {
  override val actualType: Class[String] = classOf[java.lang.String]

  override def toString = ":String"

  override def read: String => String = identity
}

case object OBJECT extends TypeDesc[Any] {
  override val actualType: Class[java.lang.Object] = classOf[java.lang.Object]

  override def toString = ":Object"

  override def show: Any => String = obj => {
    val bos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(bos)
    val bytes = try {
      oos.writeObject(obj)
      oos.flush()
      bos.toByteArray
    } finally {
      bos.close()
      oos.close()
    }
    Base64.getEncoder.encodeToString(bytes)
  }

  override def read: String => Any = str => {
    val bytes = Base64.getDecoder.decode(str)
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    try {
      ois.readObject
    } finally {
      bis.close()
      ois.close()
    }
  }
}