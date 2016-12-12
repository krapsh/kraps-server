
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
    normalizedData: Seq[GenericRowWithSchema]) {
}

/**
 * A single cell. This is how an observable is represented.
 * @param cellData the data in the cell. It can contain nulls.
 * @param cellType the type of the cell.
 */
case class CellWithType(cellData: Any, cellType: AugmentedDataType) {
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
    row match {
      case Row(x: Any) => Success(CellWithType(x, dt))
      case _ => Failure(new Exception(s"Cannot extract single row from $row, dt=$dt"))
    }
  }
}


object JsonSparkConversions {

  def deserializeCompact(adt: AugmentedDataType, data: JsValue): Try[Any] = {
    if (adt.nullability == IsNullable) {
      deserializeCompactOption(adt.dataType, data)
    } else {
      deserializeCompact0(adt.dataType, data)
    }
  }

  private def deserializeCompact0(dt: DataType, data: JsValue): Try[Any] = {
    (dt, data) match {
      case (_: IntegerType, JsNumber(n)) => Success(n.toInt)
      case (_: DoubleType, JsNumber(n)) => Success(n.toInt)
      case (_: StringType, JsString(s)) => Success(s)
      case (at: ArrayType, JsArray(arr)) =>
        if (at.containsNull) {
          val s = arr.map(e => deserializeCompactOption(at.elementType, e))
          sequence(s)
        } else {
          sequence(arr.map(e => deserializeCompact0(at.elementType, e)))
        }

      case (st: StructType, JsArray(arr)) =>
        if (st.fields.size != arr.size) {
          Failure(new Exception(s"Different sizes: $st, $arr"))
        } else {
          val fields = st.fields.zip(arr).map { case (f, e) =>
            if (f.nullable) {
              deserializeCompactOption(f.dataType, e)
            } else {
              deserializeCompact0(f.dataType, e)
            }
          }
          sequence(fields).map(Row.apply)
        }

      case (st: StructType, obj: JsObject) => deserializeObject(st, obj)

      case _ => Failure(new Exception(s"Cannot interpret data type $dt with $data"))
    }
  }

  // Some special case to allow more flexible input.
  private def deserializeObject(st: StructType, js: JsObject): Try[Row] = {
    sequence(st.fields.map { f =>
      val fun = if (f.nullable) {deserializeCompactOption _ } else { deserializeCompact0 _ }
      for {
        x <- get(js.fields, f.name)
        y <- fun(f.dataType, x)
      } yield {
        y
      }
    }).map(s => Row(s: _*))
  }

  // I think the options get replaced by nulls.
  def serializeCompact(data: Any): Try[JsValue] = data match {
    case null => Success(JsNull)
    case None => Success(JsNull)
    case Some(x) => serializeCompact(x)
    case r: Row => serializeCompact(r.toSeq)
    case seq: Seq[_] =>
      val s = sequence(seq.map(serializeCompact))
      s.map(s2 => JsArray(s2: _*))
    case i: Int => Success(JsNumber(i))
    case i: java.lang.Integer => Success(JsNumber(i))
    case l: java.lang.Long => Success(JsNumber(l))
    case d: Double => Success(JsNumber(d))
    case d: java.lang.Double => Success(JsNumber(d))
    case b: Boolean => Success(JsBoolean(b))
    case s: String => Success(JsString(s))
    case x: Any => Failure(new Exception(s"Match error type=${x.getClass} val=$x"))
  }

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

  private def deserializeCompactOption(
      dt: DataType,
      data: JsValue): Try[Option[Any]] = {
    if (data == JsNull) {
      Success(None)
    } else {
      deserializeCompact0(dt, data).map(Some.apply)
    }
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
        value <- JsonSparkConversions.deserializeCompact(adt, ct)
      } yield {
        CellWithType(value, adt)
      }
    case x: Any =>
      Failure(new Exception(s"not an object: $x"))
  }

  // Expects an object around.
  def serializeLocalCompact(row: Row): Try[JsValue] = row match {
    case Row(Array(x)) =>
      JsonSparkConversions.serializeCompact(x)
    case z =>
      Failure(new Exception(s"Not an object: $z"))
  }

  def normalizeDataTypeIfNeeded(adt: AugmentedDataType): StructType = adt match {
    case AugmentedDataType(st: StructType, IsStrict) =>
      st
    case _ => normalizeDataType(adt)
  }

  def normalizeDataType(adt: AugmentedDataType): StructType = {
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
        normalize(celldt, value)
      }
    case x: Any =>
      Failure(new Exception(s"not an object: $x"))
  }

  private def deserializeSequenceCompact(
      celldt: AugmentedDataType,
      cells: JsValue): Try[Seq[Any]] = cells match {
    case JsArray(arr) =>
      sequence(arr.map(e => JsonSparkConversions.deserializeCompact(celldt, e)))
    case x =>
      Failure(new Exception(s"Not an array: $x"))
  }

  private def normalize(adt: AugmentedDataType, res: Seq[Any]): CellCollection = adt.dataType match {
    case st: StructType =>
      val rows = res.map {
        case r: Row => new GenericRowWithSchema(r.toSeq.toArray, st)
      }
      CellCollection(adt, st, rows)
    case _ =>
      // We wrap.
      val nf = normalizeDataType(adt)
      val rows = res.map(e => new GenericRowWithSchema(Array(e), nf))
      CellCollection(adt, nf, rows)
  }

}