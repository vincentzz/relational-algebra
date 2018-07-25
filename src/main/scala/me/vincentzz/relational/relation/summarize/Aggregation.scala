package me.vincentzz.relational.relation.summarize

object Aggregation {
  def COUNT : List[_]  => Int    = _.size
  def CONCAT: List[_]  => String = _.mkString
}