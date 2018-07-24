package me.vincentzz.relational.relation.summarize

import me.vincentzz.relational.function.IFunction

object Aggregation {
  def COUNT  = IFunction({_.size}: List[_] => Int)
  def CONCAT(s: String = "")  = IFunction({_.mkString(s)}: List[String] => String)
  def SUM[T](implicit num: Numeric[T]) = IFunction({_.sum}: List[T] => T)
//  def AVG[T](implicit num: Numeric[T]) = IFunction({_.av}: List[T] => T)
//  Int
}