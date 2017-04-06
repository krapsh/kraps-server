package org.krapsh.ops


import com.typesafe.scalalogging.slf4j.{StrictLogging => Logging}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.krapsh.DataFrameWithType
import org.krapsh.ops.Extraction.{FieldName, FieldPath}
import org.krapsh.structures.{AugmentedDataType, IsNullable, IsStrict, JsonSparkConversions}
import spray.json.{JsArray, JsObject, JsValue}

import scala.util.{Failure, Success, Try}


// TODO: refactor to use ColumnWithType, it will simplify things.
object ColumnTransforms extends Logging {

  import org.krapsh.structures.JsonSparkConversions.{getString, get, sequence}

  /**
   * Starts from an unrectified dataframe represented as a column, and
   * recursively extracts the requested fields.
   *
   * @param js
   * @return
   */
  def select(adf: DataFrameWithType, js: JsValue): Try[(Seq[Column], AugmentedDataType)] = {
    // We need to unroll some computations at this level because Spark only accepts top-level
    // data structures.
    def select1(trans: StructuredTransform) = trans match {
      // Special case for the identity, because it is hard to deal with otherwise.
      case InnerOp(ColExtraction(FieldPath(Nil))) =>
        adf.rectifiedSchema.topLevelStruct match {
          case Some(st) =>
            // Unroll the computations at the top.
            val cols = st.fieldNames.map(adf.df.col).toSeq
            Success(cols -> adf.rectifiedSchema)
          case None =>
            // Extract the single column
            Success(Seq(DataFrameWithType.asColumn(adf)) -> adf.rectifiedSchema)
        }
      // Unroll the computations at the top, because we need to build the columns one by one
      case InnerStruct(fields) =>
        val cst = sequence(fields.map { f =>
          select0(adf, f.fieldTrans).map { case (c, adt2) =>
            c.as(f.fieldName.name) -> StructField(f.fieldName.name, adt2.dataType, adt2.isNullable)
          }
        })

        cst.map { cs =>
          logger.debug(s"select: cs=$cs")
          val cols = cs.map(_._1)
          val sfs = cs.map(_._2)
          val st = StructType(sfs)
          val adt = AugmentedDataType(st, IsStrict)
          logger.debug(s"select: adt=$adt")
          cols -> adt
        }
      case InnerOp(ColExtraction(x)) =>
        Failure(new Exception(s"Did not expect trans=$trans adf=$adf js=$js"))
    }

    for {
      trans <- parseTrans(js)
      res <- select1(trans)
    } yield {
      logger.debug(s"select: trans = $trans")
      logger.debug(s"select: res = $res")
      res
    }
  }

  private sealed trait ColOp
  private case class ColExtraction(path: FieldPath) extends ColOp
  private case class ColFunction(name: String, inputs: Seq[ColOp]) extends ColOp

  private case class Field(fieldName: FieldName, fieldTrans: StructuredTransform)

  private sealed trait StructuredTransform
  private case class InnerOp(op: ColOp) extends StructuredTransform
  private case class InnerStruct(fields: Seq[Field]) extends StructuredTransform

  private def parseTrans(js: JsValue): Try[StructuredTransform] = js match {
    case JsArray(arr) =>
      sequence(arr.map(parseField)).map(arr2 => InnerStruct(arr2))
    case obj: JsObject =>
      parseOp(obj).map(op => InnerOp(op))
    case x => Failure(new Exception(s"Expected array or object, got $x"))
  }

  private def parseOp(js: JsObject): Try[ColOp] = {
    def opSelect(s: String) = s match {
      case "extraction" =>
        for {
          p <- JsonSparkConversions.getFlatten(js.fields, "field")(Extraction.getFieldPath)
        } yield {
          ColExtraction(p)
        }
      case "fun" =>
        // It is a function
        for {
          fname <- JsonSparkConversions.getString(js.fields, "function")
          p <- JsonSparkConversions.getFlatten(js.fields, "args")(parseFunArgs)
        } yield {
          ColFunction(fname, p)
        }
      case s: String =>
        Failure(new Exception(s"Cannot understand op '$s' in $js"))
    }

    for {
      op <- getString(js.fields, "colOp")
      z <- opSelect(op)
    } yield z
  }

  private def parseOp(js: JsValue): Try[ColOp] = js match {
    case o: JsObject => parseOp(o)
    case _ =>
      Failure(new Exception(s"Expected object, got $js"))
  }

  private def parseFunArgs(js: JsValue): Try[Seq[ColOp]] = js match {
    case JsArray(arr) =>
      JsonSparkConversions.sequence(arr.map(parseOp))
    case _ =>
      Failure(new Exception(s"expected array, got $js"))
  }

  private def parseField(js: JsValue): Try[Field] = js match {
    case JsObject(m) =>
      for {
        fName <- getString(m, "name")
        op <- get(m, "op")
        trans <- parseTrans(op)
      } yield {
        Field(FieldName(fName), trans)
      }
    case _ =>
      Failure(new Exception(s"expected object, got $js"))
  }


  // Returns a single column. This column may need to be denormalized after that.
  private def select0(
      adf: DataFrameWithType,
      trans: StructuredTransform): Try[(Column, AugmentedDataType)] = trans match {
    case InnerOp(ColExtraction(p)) =>
      for (t <- extractType(adf.rectifiedSchema, p)) yield {
        val c = extractCol(adf, p)
        logger.debug(s"select0: t=$t c=$c")
        c -> t
      }

    case InnerStruct(fields) =>
      val fst = sequence(fields.map { f =>
        select0(adf, f.fieldTrans).map { case (c, adt2) =>
          c.as(f.fieldName.name) -> StructField(f.fieldName.name, adt2.dataType, adt2.isNullable)
        }
      })
      for (fs <- fst) yield {
        val st = StructType(fs.map(_._2))
        val str = struct(fs.map(_._1): _*)
        str -> AugmentedDataType(st, IsStrict)
      }
  }


  private def extractType(adt: AugmentedDataType, path: FieldPath): Try[AugmentedDataType] = {
    (path, adt) match {
      case (FieldPath(Nil), _) =>
        Success(adt)
      case (FieldPath(h :: t), AugmentedDataType(st: StructType, nullability)) =>
        // Look into a sub field.
        val res = st.fields.find(_.name == h.name) match {
          case None => Failure(new Exception(s"Cannot find field $h in $st"))

          case Some(StructField(_, st2: StructType, nullable, _)) if nullable =>
            // We can go deeper in the structure, but we mark the result as nullable.
            val adt2 = AugmentedDataType(st2, IsNullable)
            extractType(adt2, FieldPath(t))
          case Some(StructField(_, st2: StructType, _, _)) =>
            // We can go deeper in the structure.
            // The nullability will depend on the underlying structure.
            val adt2 = AugmentedDataType(st2, IsNullable)
            extractType(adt2, FieldPath(t))
          case Some(StructField(_, dt, nullable, _)) if t.isEmpty =>
            // We have reached the terminal node
            val nullability = if (nullable) IsNullable else IsStrict
            Success(AugmentedDataType(dt, nullability))
          case x =>
            Failure(new Exception(s"Failed to match $path in structure $st"))
        }
        if (nullability == IsNullable) {
          res.map(_.copy(nullability = IsNullable))
        } else {
          res
        }
      case _ => Failure(new Exception(s"Should be a struct: $adt for $path"))
    }
  }

  private def extractCol(adf: DataFrameWithType, path: FieldPath): Column = {
    // We are asked all the dataframe, but the schema is a primitive: go straight for the
    // unique column of this dataframe.
    (path, adf.rectifiedSchema.topLevelStruct) match {
      case (FieldPath(Nil), None) =>
        val n = adf.df.schema.fields match {
          case Array(f) => f.name
          case x => throw new Exception(s"Programming error: $x $path $adf")
        }
        adf.df.col(n)
      case (FieldPath(fname :: t), Some(st)) =>
        // Because of a bug somewhere, the first layer has to be unrolled for simple
        // projects. It is unclear to me why packing as a struct first and then accessing
        // the schema does not work.
        // We are supposed to have checked already that the path and the types are compatible.
        extractCol(adf.df.col(fname.name), FieldPath(t))
      case (FieldPath(Nil), Some(st)) =>
        logger.warn(s"Path will not work: $path and $adf")
        DataFrameWithType.asColumn(adf)
      case _ =>
        throw new Exception(s"extractCol: Invalid combination $path and $adf")
    }
  }

  // This is not checked, because the check is done when finding the type of the element.
  private def extractCol(col: Column, path: FieldPath): Column = path match {
    case FieldPath(Nil) => col
    case FieldPath(h :: t) =>
      extractCol(col.getField(h.name), FieldPath(t))
  }
}
