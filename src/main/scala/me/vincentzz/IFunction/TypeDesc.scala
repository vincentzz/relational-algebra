package me.vincentzz.IFunction

import java.util.Date

sealed abstract class TypeDesc[T] {
  def apply: Any => T = x => x.asInstanceOf[T]

  def checkType : Any => Unit = x => assert(x.isInstanceOf[T], s"Type check Failed. Found: :${x.getClass.getSimpleName}\n Expect: ${toString}")

  def display: String

  override def toString: String = display
}

object TypeDesc {
  def typeDescOf: Any => TypeDesc[_] =
    v =>
      v match {
        case v: Int    => INT
        case v: Long   => LONG
        case v: Float  => FLOAT
        case v: Char   => CHAR
        case v: Byte   => BYTE
        case v: Short  => SHORT
        case v: Double => DOUBLE
        case v: String => STRING
        case v: Date   => DATE
        case v: Any    => ANY
    }
}

case object INT    extends TypeDesc[Int] {
  override def display: String = ":Int"
}
case object LONG   extends TypeDesc[Long] {
  override def display: String = ":Long"
}
case object FLOAT  extends TypeDesc[Float] {
  override def display: String = ":Float"
}
case object CHAR   extends TypeDesc[Char] {
  override def display: String = ":Char"
}
case object BYTE   extends TypeDesc[Byte] {
  override def display: String = ":Byte"
}
case object SHORT  extends TypeDesc[Short] {
  override def display: String = ":Short"
}
case object DOUBLE extends TypeDesc[Double] {
  override def display: String = ":Double"
}
case object DATE   extends TypeDesc[Date] {
  override def display: String = ":Date"
}
case object STRING extends TypeDesc[String] {
  override def display: String = ":String"
}
case object ANY    extends TypeDesc[Any] {
  override def display: String = ":Any"
}
