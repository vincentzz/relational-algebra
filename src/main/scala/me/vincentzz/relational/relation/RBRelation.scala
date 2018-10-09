package me.vincentzz.relational.relation

import java.util.UUID

import me.vincentzz.relational.function.IFunction
import me.vincentzz.relational.typedesc.{INT, OBJECT, STRING, TypeDesc}
import me.vincentzz.util.{ListUtil, TupleUtil}

object RBRelation {
  def create(columns: List[String],
             types: List[TypeDesc[_]],
             keys: List[String],
             dataList: List[_]
            ): RBRelation = {
    val dataR = dataList.map{
      case d: List[_] => d
      case d          => TupleUtil.tupleToList(d)
    }
    RBRelation(columns, types, keys, dataR)
  }
}

case class RBRelation(
                     columns: List[String],
                     types  : List[TypeDesc[_]],
                     keys   : List[String],
                     dataR  : List[List[_]]
                     ) extends Relation {
  assert(columns.nonEmpty, "Column is empty")
  assert(!ListUtil.containsDups(columns), "Duplicates detected in column names")
  assert(columns.size == types.size, s"columns $columns mismatch with types $types")
  keys.foreach(k => assert(columns.contains(k), s"key $k is not one of column names $columns"))


  val data: List[List[_]] = dataR.map(r => {
    assert(columns.size == r.size, s"row $r size not match")
    types.zip(r).map {
      case (ty, v) => ty.cast(v)
    }
  })

  override val size: Int = data.size

  if(keys.nonEmpty && size != 0 ) {
    assert(!ListUtil.containsDups(
      data.map(r => keys.map(columns.indexOf(_)).map(r(_)))
    ), s"data duplicates on key $keys")
  }

  override def values: String => List[_] = col => {
    assert(columns.contains(col), s"column name $col is not exist")
    val colIndex = columns.indexOf(col)
    data.map(_(colIndex))
  }

  override def row: Int => Map[String, _] = i => columns.zip(data(i)).toMap

  override def newInstance(cols: List[String], types: List[TypeDesc[_]], keys: List[String], dataList: List[_]): Relation =
    RBRelation.create(cols, types, keys, dataList)


  /**
    * Project a Relation to contain specified column only
    *
    * @param cols column name
    * @return
    */
  override def project(cols: List[String]): Relation = {
    val colIdxToKeep = columns.indices.filter(i => cols.contains(columns(i)))
    val newHeader    = colIdxToKeep.map(columns(_)).toList
    val newTypes     = colIdxToKeep.map(types(_)).toList
    val newData      = data.map(r => colIdxToKeep.map(r(_)).toList)
    newInstance(newHeader, newTypes, Nil, newData).distinct(newHeader)
  }

  /**
    * extend with index number
    *
    * @param columnName column name
    * @return
    */
  override def extendWithIndex(columnName: String): Relation = {
    assert(!columns.contains(columnName), s"$columnName column already exists")
    val newHeaders = columnName :: columns
    val newTypes   = INT :: types
    val newData    = (0 until size).map(i => i :: data(i)).toList
    newInstance(newHeaders, newTypes, keys, newData)
  }

  /**
    * extend with GUID
    *
    * @param columnName column name
    * @return
    */
  override def extendWithGuid(columnName: String): Relation = {
    assert(!columns.contains(columnName), s"$columnName column already exists")
    val newHeaders = columnName :: columns
    val newTypes   = STRING :: types
    val newData    = (0 until size).map(i => UUID.randomUUID.toString :: data(i)).toList
    newInstance(newHeaders, newTypes, keys, newData)
  }

  /**
    * rename columns
    *
    * @param oldColNames old column name list
    * @param newColNames new column name list
    * @return
    */
  override def rename(oldColNames: List[String], newColNames: List[String]): Relation = {
    assert(oldColNames.size == newColNames.size, "column number mismatch")
    oldColNames.foreach(col => assert(columns.contains(col), s"there is no column $col exists"))

    val mapping    = oldColNames.zip(newColNames).toMap
    val newHeaders = columns.map(c => mapping.getOrElse(c, c))
    val newKeys    = keys.map(c => mapping.getOrElse(c, c))
    assert(!ListUtil.containsDups(newHeaders), "Duplicates detected in new column names")

    newInstance(newHeaders, types, newKeys, data)
  }

  /**
    * filter records with IFunction binding parameter of give columns
    * @param func     function to evaluate
    * @param bindCols parameter to apply to func
    * @tparam A type place holder1
    * @tparam B type place holder2
    * @return
    */
  override def restrict[A, B] (func: A => B, bindCols: List[String]): Relation = {
    val bindingIdx = bindCols.map(columns.indexOf(_))
    val newData    = data.filter(r => {
      val paramType   = bindingIdx.map(types(_))
      val paramList   = bindingIdx.map(r(_))
      val paramValues = paramType.zip(paramList).map{
        case (ty, p) => ty.cast(p)
      }
      IFunction(func)(paramValues).asInstanceOf[Boolean]
    })
    newInstance(columns, types, keys, newData)
  }

  /**
    * append rows
    *
    * @param rows list of row map
    * @return
    */
  override def appendRows(rows: List[Map[String, _]]): Relation = {
    val newData = data ++ rows.map(r => columns.map(r(_)))
    newInstance(columns, types, Nil, newData)
  }

  /**
    * Extend the Relation with value of binding columns apply to given IFunction
    * @param func function to be evaluate
    * @param bindCols parameter columns
    * @param outPutCols target column names
    * @tparam A type place holder1
    * @tparam B type place holder2
    * @return
    */
  override def extend[A,B](func: A => B, bindCols: List[String], outPutCols: List[String]): Relation = {
    bindCols.foreach(col => assert(columns.contains(col), s"there is no column $col exists"))
    val newHeader  = columns ++ outPutCols
    val newKeys    = keys
    val bindingIdx = bindCols.map(columns.indexOf(_))
    val newData    = data.map(r => {
      val paramType   = bindingIdx.map(types(_))
      val paramList   = bindingIdx.map(r(_))
      val paramValues = paramType.zip(paramList).map{
        case (ty, p) => ty.cast(p)
      }
      r ++ TupleUtil.tupleToList(IFunction(func)(paramValues))
    })
    val newTypes = newData.size match {
      case 0 => types ++ outPutCols.map(_ => OBJECT)
      case _ => types ++ outPutCols.map(c => TypeDesc.typeDescOf(newData.head(newHeader.indexOf(c))))
    }
    newInstance(newHeader, newTypes, newKeys, newData)
  }
}