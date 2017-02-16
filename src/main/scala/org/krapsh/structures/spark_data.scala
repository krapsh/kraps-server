
// Conversions of Spark data
//
// General comments:
//  - the data is transported in compact json (no struct, only arrays): a SQL datatype
//    is always required for parsing
//  - the basic unit of type in Spark is the Row with a StructType, while the basic
//    unit of data in Krapsh is the Cell with a DataType. The functions below make sure
//    that the data can be converted back and forth.
package org.krapsh.structures

import spray.json.{JsArray, JsBoolean, JsNull, JsNumber, JsObject, JsString, JsValue}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.krapsh.row.{AlgebraicRow, Cell, RowArray, RowCell}

import scala.util.{Failure, Success, Try}

sealed trait Nullable
case object IsStrict extends Nullable
case object IsNullable extends Nullable


/**
 * The basic data type used around Krapsh.
 *
 * This datatype compensates some issues with Spark datatypes, especially involving
 * nullability and strict structures at the top level.
 * @param dataType
 */
case class AugmentedDataType(dataType: DataType, nullability: Nullable) {
  def isPrimitive: Boolean = dataType match {
    case _: StructType => false
    case _ => true
  }

  /**
   * Indicates that this datatype can safely be used as a dataframe struct.
   * @return
   */
  def topLevelStruct: Option[StructType] = dataType match {
    case st: StructType if nullability == IsStrict => Some(st)
    case _ => None
  }

  def isNullable: Boolean = nullability == IsNullable
}

object AugmentedDataType {
  /**
   * Wraps the corresponding type in an array: a -> [a]
   */
  def wrapArray(adt: AugmentedDataType): AugmentedDataType = {
    AugmentedDataType(ArrayType(adt.dataType, adt.isNullable), IsStrict)
  }

  def fromField(f: StructField): AugmentedDataType = {
    val nl = if (f.nullable) IsNullable else IsStrict
    AugmentedDataType(f.dataType, nl)
  }
}

/**
 * A collection of data cells.
 * @param cellDataType the datatype of each cell, before normalization (if
 *                     normalization happened)
 * @param normalizedCellDataType the datatype of each cell, after normalization
 * @param normalizedData the data, after normalization
 */
case class CellCollection(
    cellDataType: AugmentedDataType,
    normalizedCellDataType: StructType,
    normalizedData: Seq[AlgebraicRow])

/**
 * A single cell. This is how an observable is represented.
 * @param cellData the data in the cell. It can contain nulls.
 * @param cellType the type of the cell.
 */
case class CellWithType(cellData: Cell, cellType: AugmentedDataType) {
  lazy val rowType: StructType =
    LocalSparkConversion.normalizeDataTypeIfNeeded(cellType)

  lazy val row: GenericRowWithSchema = {
    new GenericRowWithSchema(Array(cellData), rowType)
  }
}

object CellWithType {
  // Takes data, wrapped as a row following the normalization process, and exposes it
  // again as a piece of data usable by krapsh.
  def fromRow(row: Row, dt: AugmentedDataType): Try[CellWithType] = {
    // It should always have a single field for typed cells.
    dt match {
      case AugmentedDataType(st: StructType, IsStrict) =>
        for(ar <- AlgebraicRow.fromRow(row, st)) yield {
          CellWithType(RowCell(ar), dt)
        }
      case _ =>
        Failure(new Exception(s"Expected a strict struct type, got $dt"))
    }
  }
}


object JsonSparkConversions {

  def deserializeDataType(js: JsValue): Try[AugmentedDataType] = js match {
    case JsObject(m) =>
      def f(j: JsValue) = j match {
        case JsBoolean(true) => Success(IsNullable)
        case JsBoolean(false) => Success(IsStrict)
        case _ => Failure(new Exception(s"Not a boolean: $j"))
      }
      for {
        v <- get(m, "nullable")
        t <- get(m, "dt")
        b <- f(v)
        dt <- Try { DataType.fromJson(t.compactPrint) }
      } yield {
        AugmentedDataType(dt, b)
      }
    case x => Failure(new Exception(s"expected object, got $x"))
  }

  def sequence[T](xs : Seq[Try[T]]) : Try[Seq[T]] = (Try(Seq[T]()) /: xs) {
    (a, b) => a flatMap (c => b map (d => c :+ d))
  }

  def get(
      m: Map[String, JsValue],
      key: String): Try[JsValue] = m.get(key) match {
    case None => Failure(new Exception(s"Missing key $key in $m"))
    case Some(v) => Success(v)
  }

  def getBool(
      m: Map[String, JsValue],
      key: String): Try[Boolean] = m.get(key) match {
    case None => Failure(new Exception(s"Missing key $key in $m"))
    case Some(JsBoolean(b)) => Success(b)
    case Some(x) => Failure(new Exception(s"Wrong value $x for key $key in $m"))
  }

  def getObject(m: Map[String, JsValue], key: String): Try[JsObject] = {
    getFlatten(m, key) {
      case x: JsObject => Success(x)
      case x => Failure(new Exception(s"Expected object, got $x"))
    }
  }

  def getFlatten[X](m: Map[String, JsValue], key: String)(f: JsValue => Try[X]): Try[X] = {
    m.get(key) match {
      case None => Failure(new Exception(s"Missing key $key in $m"))
      case Some(x) => f(x)
    }
  }

  def getFlattenSeq[X](m: Map[String, JsValue], key: String)(f: JsValue => Try[X]): Try[Seq[X]] = {
    def f2(jsValue: JsValue) = jsValue match {
      case JsArray(arr) => sequence(arr.map(f))
      case x => Failure(new Exception(s"Expected array, got $x"))
    }
    getFlatten(m, key)(f2)
  }

  def getString(
      m: Map[String, JsValue],
      key: String): Try[String] = m.get(key) match {
    case None => Failure(new Exception(s"Missing key $key in $m"))
    case Some(JsString(s)) => Success(s)
    case Some(x) => Failure(new Exception(s"Wrong value $x for key $key in $m"))
  }

  def getStringList(
      m: Map[String, JsValue],
      key: String): Try[List[String]] = m.get(key) match {
    case None => Failure(new Exception(s"Missing key $key in $m"))
    case Some(JsArray(arr)) => sequence(arr.map {
      case JsString(s) => Success(s)
      case x => Failure(new Exception(s"Expected string, got $x"))
    }).map(_.toList)
    case Some(x) => Failure(new Exception(s"Wrong value $x for key $key in $m"))
  }

  def getStringListList(
      m: Map[String, JsValue],
      key: String): Try[Seq[Seq[String]]] = m.get(key) match {
    case None => Failure(new Exception(s"Missing key $key in $m"))
    case Some(JsArray(arr)) => sequence(arr.map(arr2 =>
      getStringList(Map("k"->JsArray(arr2)), "k")))
    case Some(x) => Failure(new Exception(s"Wrong value $x for key $key in $m"))
  }

}

object LocalSparkConversion {

  import JsonSparkConversions.get

  // In every case, it wraps the content in an object.
  def deserializeLocal(js: JsValue): Try[CellWithType] = js match {
    case JsObject(m) =>
      for {
        tp <- get(m, "type")
        ct <- get(m, "content")
        adt <- JsonSparkConversions.deserializeDataType(tp)
        value <- AlgebraicRow.fromJson(ct, adt)
      } yield {
        CellWithType(value, adt)
      }
    case x: Any =>
      Failure(new Exception(s"not an object: $x"))
  }

  /**
   * Takes an augmented data type and attempts to convert it to a top-level struct that is
   * compatible with Spark data representation: strict top-level structures go through, everything
   * else is wrapped in a top-level struct with a single field.
   */
  def normalizeDataTypeIfNeeded(adt: AugmentedDataType): StructType = adt match {
    case AugmentedDataType(st: StructType, IsStrict) =>
      st
    case _ => normalizeDataType(adt)
  }

  private def normalizeDataType(adt: AugmentedDataType): StructType = {
    val f = StructField(
      "_1",
      adt.dataType, nullable = adt.nullability == IsNullable,
      metadata = Metadata.empty)
    StructType(Array(f))
  }
}

/**
 * Performs a number of conversions between the data as represented by krapsh
 * and the data accepted by Spark.
 *
 * All the data in a dataframe or in a row has to be a struct, but krapsh also
 * accepts primitive, non-nullable datatypes to be represented in a dataframe
 * or in a row. This is why there is a normalization process.
 * All data which is a non-nullable, primitive type associated with column name "_1
 * is automatically converted back and forth with the corresponding primitive type
 * The name _1 is already reserved for tuples, so it should not cause some ambiguity.
 *
 * All the observables are unconditionally wrapped into a structure with a single
 * non-nullable field called _1. This allows a representation that handles both
 * primitive and non-primitive types in a uniform manner.
 */
object DistributedSparkConversion {

  import JsonSparkConversions.{get, sequence}
  import LocalSparkConversion.normalizeDataType

  /**
   * Deserializes, with a normalization process to try to
   * keep data structures while allowing primitive types.
   * @param js a pair of cell data types and some cells.
   */
  def deserializeDistributed(js: JsValue): Try[CellCollection] = js match {
    case JsObject(m) =>
      for {
        tp <- get(m, "cellType")
        ct <- get(m, "content")
        celldt <- JsonSparkConversions.deserializeDataType(tp)
        value <- deserializeSequenceCompact(celldt, ct)
      } yield {
        val rows = value.map(normalizeCell)
        val st = LocalSparkConversion.normalizeDataTypeIfNeeded(celldt)
        CellCollection(celldt, st, rows)
      }
    case x: Any =>
      Failure(new Exception(s"not an object: $x"))
  }

  /**
   * Deserializes the content of a sequence (which should be a sequence of cells)
   */
  private def deserializeSequenceCompact(
      celldt: AugmentedDataType,
      cells: JsValue): Try[Seq[Cell]] = cells match {
    case JsArray(arr) =>
      sequence(arr.map(e => AlgebraicRow.fromJson(e, celldt)))
    case x =>
      Failure(new Exception(s"Not an array: $x"))
  }

  /**
   * Takes a cell data and build a normalized representation of it: top-level rows go through,
   * everything else is wrapped in a row.
   * @param c
   * @return
   */
  private def normalizeCell(c: Cell): AlgebraicRow = c match {
    case RowCell(r) => r
    case _ => AlgebraicRow(Seq(c))
  }
}