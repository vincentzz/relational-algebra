package me.vincentzz.relational.relation.persist

import java.io.{File, PrintWriter}

import me.vincentzz.relational.relation.{RBRelation, Relation}
import me.vincentzz.relational.typedesc.TypeDesc
import me.vincentzz.util.StringUtil

import scala.annotation.tailrec
import scala.io.Source

case class CSVConfig(quote: Char, separator: Char, headerLines: Int)

object CSV {
  val escapeChar = '\\'
  val defaultCSVConfig = CSVConfig('\"', ',', 1)

  def readFromCSV(fn: String,
                  columns: List[String],
                  types: List[TypeDesc[_]],
                  csvConfig: CSVConfig = defaultCSVConfig): Relation = {
    val source  = Source.fromFile(fn)
    val content = try {
      source.getLines().toList
    } finally source.close()
    assert(content.size >= csvConfig.headerLines, s"CSV file line count less than ${csvConfig.headerLines}")

    @tailrec
    def parseLine(parsed     : List[String],
                  currentWord: String,
                  left       : List[Char],
                  inQuote    : Boolean,
                  closeQuote : Boolean,
                  escape     : Boolean): List[String] = {
      left match {
        case c::cs if c == csvConfig.separator && !inQuote =>
          parseLine(parsed:+ StringUtil.treatEscapes(currentWord), "", cs,inQuote = false, closeQuote = false, escape = false)
        case c::_  if !inQuote && closeQuote && !(c == '\t' || c == ' ') =>
          throw new IllegalArgumentException(
            s"cannot parse CSV line: ${parsed.mkString(csvConfig.separator.toString)}$currentWord${left.mkString}")
        case c::cs if !inQuote && closeQuote && (c == '\t' || c == ' ') =>
          parseLine(parsed, currentWord, cs, inQuote, closeQuote, escape = false)
        case c::cs if c == csvConfig.quote && !inQuote && !escape && currentWord.trim == "" =>
          parseLine(parsed, "", cs, inQuote = true, closeQuote = false, escape = false)
        case c::cs if c == csvConfig.quote && inQuote && !escape =>
          parseLine(parsed, currentWord, cs, inQuote = false, closeQuote = true, escape = false)
        case c::cs if inQuote && c == escapeChar =>
          parseLine(parsed, currentWord + c.toString, cs, inQuote, closeQuote, escape = true)
        case c::cs if c == csvConfig.quote && escape =>
          parseLine(parsed, currentWord + c.toString, cs, inQuote, closeQuote, escape = false)
        case c::cs =>
          parseLine(parsed, currentWord + c.toString, cs, inQuote, closeQuote, escape = false)
        case Nil   => parsed :+ StringUtil.treatEscapes(currentWord)
      }
    }

    val table    = content.map(l => parseLine(Nil, "", l.toCharArray.toList, inQuote = false, closeQuote = false, escape = false))
    val strTable = csvConfig.headerLines match {
      case 0 => table
      case n =>
        val hi = columns.map(table.head.indexOf(_))
        assert(!hi.contains(-1), s"CSV column name not match... expecting: ${columns.mkString(",")}, actual: ${table.head.mkString(",")}")
        table.drop(n).map(row => hi.map(row(_)))
    }

    val data = strTable.map(row => types.zip(row).map{
      case (ty, str) => ty.read(str)
    })

    RBRelation.create(columns, types, Nil, data)
  }

  def saveToCSV(fn: String,
                rel: Relation,
                csvConfig: CSVConfig = defaultCSVConfig): Unit = {
    val pw = new PrintWriter(new File(fn))
    try {
      pw.println(
        rel.columns.map(h => {
          val containsQuote     = h.contains(csvConfig.quote)
          val containsSeparator = h.contains(csvConfig.separator)
          val s = StringUtil.toEscapes(h.trim)

          if (containsQuote || containsSeparator) {
            s"${csvConfig.quote.toString}$s${csvConfig.quote.toString}"
          } else s
        }).mkString(csvConfig.separator.toString)
      )

      rel.foreach(row => {
        pw.println(
          rel.columns.map(h => {
            val c = rel.typeOf(h).show(row(h))
            val containsQuote     = c.contains(csvConfig.quote)
            val containsSeparator  = c.contains(csvConfig.separator)
            val s = StringUtil.toEscapes(c.trim)

            if (containsQuote || containsSeparator) {
              s"${csvConfig.quote.toString}$s${csvConfig.quote.toString}"
            } else s
          }).mkString(csvConfig.separator.toString)
        )
      })
    } finally pw.close()
  }
}



































































