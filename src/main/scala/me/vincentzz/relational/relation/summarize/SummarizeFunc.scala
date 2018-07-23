package me.vincentzz.relational.relation.summarize

import me.vincentzz.relational.function.IFunction

object SummarizeFunc {
  def COUNT  = IFunction({_.size}: List[_] => Int)
  def CONCATENATE(s: String)  = IFunction({_.mkString(s)}: List[String] => String)
  def SUM[T](implicit num: Numeric[T]) = IFunction({_.sum}: List[T] => T)
}

