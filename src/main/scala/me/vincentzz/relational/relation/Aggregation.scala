package me.vincentzz.relational.relation

import org.joda.time.DateTime

object Aggregation {
  def COUNT        : List[_]        => Int    = _.size
  def CONCAT       : List[_]        => String = _.mkString

  def SUM_Int      : List[Int]      => Int    = _.sum
  def SUM_Double   : List[Double]   => Double = _.sum
  def SUM_Float    : List[Float]    => Float  = _.sum
  def SUM_Short    : List[Short]    => Short  = _.sum
  def SUM_Long     : List[Long]     => Long   = _.sum


  def AVG         : List[_]         => Double   = xs => xs.map {
    case x: Int    => x.toDouble
    case x: Short  => x.toDouble
    case x: Float  => x.toDouble
    case x: Long   => x.toDouble
    case x: Double => x
  }.sum / xs.size

  def MIN_Int      : List[Int]      => Int      = _.min
  def MIN_Double   : List[Double]   => Double   = _.min
  def MIN_Float    : List[Float]    => Float    = _.min
  def MIN_Short    : List[Short]    => Short    = _.min
  def MIN_Long     : List[Long]     => Long     = _.min
  def MIN_Char     : List[Char]     => Char     = _.min
  def MIN_Byte     : List[Byte]     => Byte     = _.min
  def MIN_String   : List[String]   => String   = _.min
  def MIN_Date     : List[DateTime] => DateTime = dts => new DateTime(dts.map(_.getMillis).min)

  def MAX_Int      : List[Int]      => Int      = _.max
  def MAX_Double   : List[Double]   => Double   = _.max
  def MAX_Float    : List[Float]    => Float    = _.max
  def MAX_Short    : List[Short]    => Short    = _.max
  def MAX_Long     : List[Long]     => Long     = _.max
  def MAX_Char     : List[Char]     => Char     = _.max
  def MAX_Byte     : List[Byte]     => Byte     = _.max
  def MAX_String   : List[String]   => String   = _.max
  def MAX_Date     : List[DateTime] => DateTime = dts => new DateTime(dts.map(_.getMillis).max)

  def MEDIAN_Int   : List[Int]   => Int      = {
    case xs if xs.size%2==1 => xs.sorted.slice(xs.size/2  ,xs.size/2+1).head
    case xs if xs.size%2==0 => xs.sorted.slice(xs.size/2-1,xs.size/2+1).sum/2
  }
  def MEDIAN_Double: List[Double] => Double   = {
    case xs if xs.size%2==1 => xs.sorted.slice(xs.size/2  ,xs.size/2+1).head
    case xs if xs.size%2==0 => xs.sorted.slice(xs.size/2-1,xs.size/2+1).sum/2
  }
  def MEDIAN_Float : List[Float]    => Float    = {
    case xs if xs.size%2==1 => xs.sorted.slice(xs.size/2  ,xs.size/2+1).head
    case xs if xs.size%2==0 => xs.sorted.slice(xs.size/2-1,xs.size/2+1).sum/2
  }
  def MEDIAN_Long  : List[Long]     => Long     = {
    case xs if xs.size%2==1 => xs.sorted.slice(xs.size/2  ,xs.size/2+1).head
    case xs if xs.size%2==0 => xs.sorted.slice(xs.size/2-1,xs.size/2+1).sum/2
  }

}
