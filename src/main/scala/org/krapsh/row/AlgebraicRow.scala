package org.krapsh.row


import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, StructType}

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
// Unlike the haskell code, we need to make a distinction between the
// row and the array case during the reconstruction.
case class RowArray(seq: Seq[Cell]) extends Cell
case class RowCell(r: AlgebraicRow) extends Cell

object AlgebraicRow {

  import org.krapsh.structures.JsonSparkConversions.sequence

  def fromRow(r: Row, st: StructType): Try[AlgebraicRow] = {
    from(r.toSeq, st) match {
      case Success(RowCell(c)) => Success(c)
      case Success(x) => Failure(new Exception(s"Got $x from $st -> $r"))
      case Failure(e) => Failure(e)
    }
  }

  def toRow(ar: AlgebraicRow): Row = Row(ar.cells.map(toAny))

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

