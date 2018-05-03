package me.vincentzz.IFunction

trait IRelation {
  def columns : List[String]
  def types   : List[TypeDesc[_]]
  def keys    : List[String]
  def size    : Int
  def values  : String => List[Any]
  def rows    : Int    => Map[String, Any]
}
object IRelation {
  def createR(columns: List[String], types: List[TypeDesc[_]], keys: List[String], dataList: List[Any]): IRelation = {
    assert(columns.size == types.size, s"Column ${columns} mismatch with Types ${types}")
    assert(keys.map(columns.contains(_)).foldRight(true)(_ && _), s"Keys ${keys} should be subset of Columns ${columns}")
    val dataMaps = dataList.map(data =>
      data match {
        case d: Product => {
          val dList = d.productIterator.toList
          assert(columns.size == dList.size, s"Columns ${columns} mismatch with Data ${dList}")
          types.zip(d.productIterator.toList).map{case (ty, value) => ty.checkType(value)}
          columns.zip(dList).toMap
        }
        case x: Any     => {
          assert(columns.size == 1)
          Map(columns(0) -> x)
        }
      })
    RRelation(columns, types, keys, dataMaps)
  }


//  def createC(columns: List[String], types: List[TypeDesc[_]], keys: List[String], dataList: List[Any]): IRelation = {
//    CRelation()
//  }
}
private case class RRelation(columns: List[String], types: List[TypeDesc[_]], keys: List[String], data: List[Map[String, Any]])extends IRelation {

  override def size: Int = data.size

  override def values: String => List[Any] = col => data.map(_(col))

  override def rows: Int => Map[String, Any] = i => data(i)
}

private case class CRelation()