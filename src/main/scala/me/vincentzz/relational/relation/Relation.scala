package me.vincentzz.relational.relation

import me.vincentzz.relational.function.IFunction
import me.vincentzz.relational.typedesc.{OBJECT, TypeDesc}


trait Relation extends Traversable[Map[String,_]] {
  def columns : List[String]
  def types   : List[TypeDesc[_]]
  def keys    : List[String]
  def values  : String => List[_]
  def row     : Int    => Map[String, _]

  def newInstance(cols: List[String], types: List[TypeDesc[_]], keys: List[String], dataList: List[_]): Relation

  /**
    * return the type description of given column
    * @param col column name
    * @return
    */
  def typeOf(col: String): TypeDesc[_] = types(columns.indexOf(col))

  /**
    * Project a Relation to contain specified column only
    * @param cols column name
    * @return
    */
  def project(cols: List[String]): Relation

  /**
    * Get new Relation with specified keys
    * @param newKeys new Key column list
    * @return
    */
  def withKeys(newKeys: List[String]): Relation = newInstance(columns, types, newKeys, (0 until size).map(i => columns.map(row(i)(_))).toList)

  /**
    * extend with index number
    * @param columnName column name
    * @return
    */
  def extendWithIndex(columnName: String): Relation

  /**
    * extend with GUID
    * @param columnName column name
    * @return
    */
  def extendWithGuid(columnName: String): Relation

  /**
    * rename columns
    * @param oldColNames old column name list
    * @param newColNames new column name list
    * @return
    */
  def rename(oldColNames: List[String], newColNames: List[String]): Relation

  /**
    * filter records with IFunction binding parameter of give columns
    * @param func     function to evaluate
    * @param bindCols parameter to apply to func
    * @return
    */
  def restrict(func: IFunction, bindCols: List[String]): Relation


  /**
    * return true if any row matches given record exactly
    * @param record Map of (column -> value) pair
    * @return
    */
  def contains(record: Map[String, _]): Boolean = (0 until size).foldLeft(false)((r, i) => r || (row(i) == record))

  /**
    * return true if any row contains given record
    * @param record Map of (column -> value) pair
    * @return
    */
  def appears(record: Map[String, _]): Boolean = (0 until size).foldLeft(false)((r, i) => r || record.toSet.subsetOf(row(i).toSet))

  /**
    * Traverse by rows
    * @param f  function
    * @tparam U return type
    */
  override def foreach[U](f: Map[String, _] => U): Unit = (0 until size).map(row).foreach(f)

  /**
    * mapping by rows
    * @param f function
    * @tparam T return type
    * @return
    */
  def map[T](f: Map[String, _] => T): IndexedSeq[T] = (0 until size).map(row).map(f)

  /**
    * filter rows with condition map
    * @param condition condition map
    * @return
    */
  def filter(condition: Map[String, _]): Relation = {
    condition.keySet.foreach(col => assert(columns.contains(col), s"column $col not exist"))
    val newData = (0 until size).filter(i => {
      row(i).filterKeys(condition.keySet.contains(_)) == condition
    }).map(r => columns.map(row(r))).toList
    newInstance(columns, types, keys, newData)
  }

  /**
    * append rows
    * @param rows list of row map
    * @return
    */
  def appendRows(rows: List[Map[String, _]]): Relation

  /**
    * select given columns and remove duplicates
    * @param cols column names
    * @return
    */
  def distinct(cols: List[String]): Relation = {
    cols.foreach(col => assert(columns.contains(col), s"column $col not exist"))
    val newTypes = cols.map(typeOf)
    val newData  = (0 until size).map(i => {
      row(i).filterKeys(cols.contains)
    }).toSet[Map[String, _]].toList.map(r => cols.map(r(_)))
    newInstance(cols, newTypes, Nil, newData)
  }

  /**
    * Relational union operation
    * Returns a new relation contains all records that appears in either or both of two relations.
    * @param that the other relation
    * @return
    */
  def union(that: Relation): Relation = {
    assert(this.columns.toSet == that.columns.toSet, "columns does not match.")
    assert(this.types   == that.types  , "column types does not match.")
    val newRows = (0 until that.size).filter( i => {
      !this.contains(that.row(i))
    }).map(that.row).toList
    appendRows(newRows)
  }

  /**
    * Relational natural join operation
    * @param that the other relation
    * @return
    */
  def join(that: Relation): Relation = {
    val matchCols = this.columns.filter(that.columns.contains(_))
    matchCols.foreach(col => assert(this.typeOf(col) == that.typeOf(col), s"type mismatch on column $col"))

    val thisCols   = this.columns.filter(!matchCols.contains(_))
    val thatCols   = that.columns.filter(!matchCols.contains(_))

    val newHeaders = matchCols ++ thisCols ++ thatCols
    val newType    = (matchCols ++ thisCols).map(this.typeOf) ++ that.columns.map(that.typeOf)

    val newData: List[List[_]] = (for {
      i1 <- 0 until this.size
      i2 <- 0 until that.size
      if matchCols.map(this.row(i1)(_)) == matchCols.map(that.row(i2)(_))
    } yield {
      (matchCols ++ thisCols).map(this.row(i1)(_)) ++ thatCols.map(that.row(i2)(_))
    }).toList

    newInstance(newHeaders, newType, Nil, newData)
  }

  /**
    * Relational semi-minus operation
    * @param that the other relation
    * @return
    */
  def semiJoin(that: Relation): Relation = join(that).project(this.columns)

  /**
    * Relational difference operator
    * @param that the other relation
    * @return
    */
  def semiMinus(that: Relation): Relation = {
    val matchCols = this.columns.filter(that.columns.contains)
    val newData   = (for {
      i <- 0 until this.size
      if !that.appears(matchCols.map(col => col -> this.row(i)(col)).toMap)
    } yield columns.map(this.row(i)(_))).toList
    newInstance(columns, types, keys, newData)
  }

  /**
    * Relational difference operator
    * @param that the other relation
    * @return
    */
  def minus(that: Relation): Relation = {
    assert(this.columns.sorted == that.columns.sorted, "column names mismatch")
    this.columns.foreach(col => assert(this.typeOf(col) == that.typeOf(col), s"type of $col mismatch"))
    semiMinus(that)
  }

  /**
    * Extend the Relation with value of binding columns apply to given IFunction
    * @param func IFunction
    * @param bindCols parameter columns
    * @param outPutCols target column names
    * @return
    */
  def extend(func: IFunction, bindCols: List[String], outPutCols: List[String]): Relation

  /**
    * drop specified columns
    * @param cols column names to drop
    * @return
    */
  def allBut(cols: List[String]): Relation = {
    cols.foreach(col => assert(columns.contains(col), s"$col not exist in $columns"))
    project(columns.filter(cols.contains))
  }

  def summarize(groupCols: List[String], op: IFunction, col: String, outPutCol: String): Relation = {
    groupCols.foreach(col => assert(columns.contains(col), s"column $col not exists"))
    assert(!groupCols.contains(outPutCol), s"column $outPutCol exists in grouping columns")

    val newHeader  = groupCols :+ outPutCol
    val dis_rel    = distinct(groupCols)
    val newKeys    = Nil

    val ty         = typeOf(col)
    val newData    = (0 until dis_rel.size).map(i => {
      groupCols.map(dis_rel.row(i)(_)) :+ op(List(filter(dis_rel.row(i)).values(col)))
    }).toList

    val newTypes = groupCols.map(typeOf(_)) :+ (newData.size match {
      case 0 => OBJECT
      case _ => TypeDesc.typeDescOf(newData.head.last)
    })

    newInstance(newHeader, newTypes, newKeys, newData)
  }

  /**
    * pretty toString
    * @return
    */
  override def toString: String = {
    val NEW_LINE = "\n"
    val colWidths = columns.map { col =>
      val headerLen = col.length
      val typeLen   = typeOf(col).toString.length

      val dataMaxWide = size match {
        case 0 => 0
        case _ => (0 until size).map {
          row(_)(col).toString.split(NEW_LINE).map(_.length).max
        }.max
      }
      List (dataMaxWide, headerLen, typeLen).max
    }

    val headerHeight = columns.map(_.split(NEW_LINE).length).max
    val typeHeight   = types.map(_.toString.split(NEW_LINE).length).max
    val rowHeight   = headerHeight :: typeHeight :: {
      (0 until size).map {
        i => columns.map(row(i)(_).toString.split(NEW_LINE).length).max
      }.toList
    }

    val bolder = "+" + colWidths.map(n => s"${"-" * (n+2)}").mkString("+") + "+"

    val toPrint = columns :: types :: (0 until size).map(i => columns.map(row(i)(_))).toList

    bolder + NEW_LINE + toPrint.zip(rowHeight).map {
      case (r, h) => {
        (0 until h).map( ri => {
          r.zip(colWidths).map {
            case (c, n) => {
              val content = c.toString.split(NEW_LINE)
              val height  = content.size
              val preEmLine  = (h - height) / 2
              val postEmLine = h - height - preEmLine
              val wide = content.map(_.length).max
              val headSpace = (n - wide) / 2

              if (ri < preEmLine || ri >= h- postEmLine) f"| ${" " * n } "
              else {
                val i = h - postEmLine -ri
                val l = height - i
                val tailSpace = n - headSpace - content(l).length
                f"| ${" " * headSpace}${content(l)}${" " * tailSpace} "
              }
            }.mkString
          }.mkString + "|"
        }).mkString(NEW_LINE)
      }.mkString
    }.mkString(NEW_LINE + bolder + NEW_LINE) + NEW_LINE + bolder
  }
}
