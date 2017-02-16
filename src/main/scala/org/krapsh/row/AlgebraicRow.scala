package org.krapsh.row


import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.krapsh.structures.{AugmentedDataType, IsNullable}
import spray.json.{JsArray, JsNull, JsNumber, JsObject, JsString, JsValue}

import scala.util.{Failure, Success, Try}

/**
 * A representation of a row that is easy to manipulate with
 * algebraic datatypes.
 */
case class AlgebraicRow(cells: Seq[Cell])

sealed trait Cell
case object Empty extends Cell // The null elements
case class IntElement(i: Int) extends Cell
case class StringElement(s: String) extends Cell
case class BoolElement(b: Boolean) extends Cell
// Unlike the haskell code, we need to make a distinction between the
// row and the array case during the reconstruction.
case class RowArray(seq: Seq[Cell]) extends Cell
case class RowCell(r: AlgebraicRow) extends Cell

object AlgebraicRow {

  import org.krapsh.structures.JsonSparkConversions.{sequence, get}

  def fromRow(r: Row, st: StructType): Try[AlgebraicRow] = {
    from(r.toSeq, st) match {
      case Success(RowCell(c)) => Success(c)
      case Success(x) => Failure(new Exception(s"Got $x from $st -> $r"))
      case Failure(e) => Failure(e)
    }
  }

  def toRow(ar: AlgebraicRow): Row = Row(ar.cells.map(toAny):_*)

  def fromJson(js: JsValue, adt: AugmentedDataType): Try[Cell] = {
    if (adt.nullability == IsNullable) {
      deserializeCompactOption(adt.dataType, js)
    } else {
      deserializeCompact0(adt.dataType, js)
    }
  }

  private def deserializeCompactOption(
      dt: DataType,
      data: JsValue): Try[Cell] = {
    if (data == JsNull) {
      Success(Empty)
    } else {
      deserializeCompact0(dt, data)
    }
  }

  private def deserializeCompact0(dt: DataType, data: JsValue): Try[Cell] = {
    (dt, data) match {
      case (_: IntegerType, JsNumber(n)) => Success(IntElement(n.toInt))
      case (_: DoubleType, JsNumber(n)) => Success(IntElement(n.toInt))
      case (_: StringType, JsString(s)) => Success(StringElement(s))
      case (at: ArrayType, JsArray(arr)) =>
        if (at.containsNull) {
          val s = arr.map(e => deserializeCompactOption(at.elementType, e))
          sequence(s).map(RowArray.apply)
        } else {
          sequence(arr.map(e => deserializeCompact0(at.elementType, e)))
            .map(RowArray.apply)
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
          sequence(fields).map(seq => RowCell(AlgebraicRow(seq)))
        }

      case (st: StructType, obj: JsObject) => deserializeObject(st, obj)

      case _ => Failure(new Exception(s"Cannot interpret data type $dt with $data"))
    }
  }

  // Some special case to allow more flexible input.
  private def deserializeObject(st: StructType, js: JsObject): Try[Cell] = {
    sequence(st.fields.map { f =>
      val fun = if (f.nullable) {deserializeCompactOption _ } else { deserializeCompact0 _ }
      for {
        x <- get(js.fields, f.name)
        y <- fun(f.dataType, x)
      } yield {
        y
      }
    }).map(s => RowCell(AlgebraicRow(s)))
  }

  private def cellsOrdering = Ordering.Iterable[Cell](CellOrdering)

  // Defines a canonical ordering across any row and any cell.
  // The content need not match
  object RowOrdering extends Ordering[AlgebraicRow] {
    override def compare(x: AlgebraicRow, y: AlgebraicRow): Int = {
      cellsOrdering.compare(x.cells, y.cells)
    }
  }

  // TODO: should it be made to match the correct values only?
  // Arbitrary stuff may mask some bugs.
  object CellOrdering extends Ordering[Cell] {
    override def compare(x: Cell, y: Cell): Int = (x, y) match {
      case (IntElement(i1), IntElement(i2)) =>
        Ordering.Int.compare(i1, i2)
      case (IntElement(_), _) => 1
      case (_, IntElement(_)) => -1
      case (StringElement(s1), StringElement(s2)) =>
        Ordering.String.compare(s1, s2)
      case (StringElement(_), _) => 1
      case (_, StringElement(_)) => -1
      case (BoolElement(b1), BoolElement(b2)) =>
        Ordering.Boolean.compare(b1, b2)
      case (BoolElement(_), _) => 1
      case (_, BoolElement(_)) => -1
      case (RowArray(seq1), RowArray(seq2)) =>
        cellsOrdering.compare(seq1, seq2)
      case (RowArray(_), _) => 1
      case (_, RowArray(_)) => -1
      case (RowCell(c1), RowCell(c2)) =>
        cellsOrdering.compare(c1.cells, c2.cells)
      case (RowCell(_), _) => 1
      case (_, RowCell(_)) => -1
      case (Empty, Empty) => 0
    }
  }

  def toAny(c: Cell): Any = c match {
    case Empty => null
    case IntElement(i) => i
    case StringElement(s) => s
    case RowArray(s) => s.map(toAny)
    case RowCell(r) => toRow(r)
    case BoolElement(b) => b
  }

  private def from(x: Any, dt: DataType): Try[Cell] = (x, dt) match {
    // Nulls, etc.
    case (null, _) => Success(Empty)
    case (None, _) => Success(Empty)
    case (Some(null), _) => Success(Empty)
    case (Some(y), _) => from(y, dt)
    // Primitives
    case (i: Int, t: IntegerType) => Success(IntElement(i))
    case (i: Integer, t: IntegerType) => Success(IntElement(i))
    // TODO: proper implementation of the long type
    case (i: Long, t: LongType) => Success(IntElement(i.toInt))
    case (s: String, t: StringType) => Success(StringElement(s))
    // Sequences
    case (a: Array[Any], _) => from(a.toSeq, dt)
    case (s: Seq[Any], t: ArrayType) =>
      sequence(s.map(from(_, t.elementType))).map(RowArray.apply)
    // Structures
    case (r: Row, t: StructType) =>
      from(r.toSeq, t)
    case (s: Seq[Any], t: StructType) =>
      val elts = s.zip(t.fields.map(_.dataType))
        .map { case (x2, dt2) => from(x2, dt2) }
      sequence(elts).map(ys => RowCell(AlgebraicRow(ys)))
    case _ => Failure(new Exception(s"Datatype $dt is not compatible with " +
      s"value type ${x.getClass}: $x"))
  }
}

