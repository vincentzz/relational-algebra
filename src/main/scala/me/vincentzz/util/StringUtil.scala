package me.vincentzz.util

object StringUtil {
  val escapeCharList = List(
    "\\\\" -> "\\\\\\\\",
    "\n"   -> "\\\\n",
    "\t"   -> "\\\\t",
    "\r"   -> "\\\\r",
    "\b"   -> "\\\\b",
    "\'"   -> "\\\\\'",
    "\""   -> "\\\\\"",
    "\f"   -> "\\\\f"
  )

  def toEscapes(s: String): String = escapeCharList.foldLeft(s)((temp, x) => x match {
    case (origin, replacement) => temp.replaceAll(origin, replacement)
  })

  def treatEscapes(s: String): String = StringContext.treatEscapes(s)

}
