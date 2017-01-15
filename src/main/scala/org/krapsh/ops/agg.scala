package org.krapsh.ops

import scala.util.{Failure, Try}

import com.typesafe.scalalogging.slf4j.{StrictLogging => Logging}
import spray.json.{JsArray, JsObject, JsValue}

import org.apache.spark.sql.types._
import org.apache.spark.sql.RelationalGroupedDataset

import org.krapsh.{ColumnWithType, DataFrameWithType}
import org.krapsh.ops.Extraction.{FieldName, FieldPath}
import org.krapsh.ops.SQLFunctionsExtraction.SQLFunctionName
import org.krapsh.structures.{AugmentedDataType, IsStrict, JsonSparkConversions}


object GroupedReduction extends Logging {

  import org.krapsh.structures.JsonSparkConversions.{getString, get, sequence}

  def groupReduceOrThrow(adf: DataFrameWithType, js: JsValue): DataFrameWithType =
    groupReduce(adf, js).get

  def groupReduce(adf: DataFrameWithType, js: JsValue): Try[DataFrameWithType] = {
    for {
      op <- parseTrans(js)
      (g, valCol) <- makeGroup(adf)
      col <- performTrans(valCol, op)
    } yield {
      val df = g.agg(col.col.alias("value"))
      DataFrameWithType(df, AugmentedDataType.apply(df.schema, IsStrict))
    }
  }

  def reduceOrThrow(adf: DataFrameWithType, js: JsValue): DataFrameWithType =
    reduce(adf, js).get

  def reduce(adf: DataFrameWithType, js: JsValue): Try[DataFrameWithType] = {
    val c = DataFrameWithType.asTypedColumn(adf)
    logger.info(s"reduce: c=$c adf=$adf")
    for {
      op <- parseTrans(js)
      col <- performTrans(c, op)
    } yield {
      val df = adf.df.groupBy().agg(col.col)
      DataFrameWithType(df, col.rectifiedSchema)
    }
  }


  private sealed trait AggOp
  private case class AggFunction(function: SQLFunctionName, inputs: Seq[FieldPath]) extends AggOp
  private case class AggStruct(struct: Seq[Field]) extends AggOp

  private case class Field(fieldName: FieldName, op: AggOp)

  // Groups all the keys together under a single "key' field and performs a group under this field.
  private def makeGroup(
      adf: DataFrameWithType): Try[(RelationalGroupedDataset, ColumnWithType)] = {
    // We expect a structure with 2 elements inside:
    val df = adf.df
    df.schema match {
      case StructType(Array(f1, f2)) =>
        val c = DataFrameWithType.asTypedColumn(adf)
        val g = df.groupBy(f1.name)
        for {
          c2 <- Extraction.extractField(c, FieldName(f2.name))
        } yield g -> c2
      case x => Failure(new Exception(s"Expected a struct with two fields, got $x"))
    }
  }

  private def parseTrans(js: JsValue): Try[AggOp] = {
    js match {
      case JsArray(arr) =>
        sequence(arr.map(parseField)).map(AggStruct.apply)
      case JsObject(m) => parseOp(m)
      case x => Failure(new Exception(s"parseTrans: unexpected object $x"))
    }
  }

  private def parseOp(m: Map[String, JsValue]): Try[AggOp] = {
    def opSelect(s: String) = s match {
      case "function" =>
        for {
          l <- JsonSparkConversions.getFlattenSeq(m, "fields")(Extraction.getFieldPath)
          n <- getString(m, "functionName")
        } yield AggFunction(n, l)
      case n =>
        Failure(new Exception(s"Operation $s not understood"))
    }
    for {
      op <- getString(m, "aggOp")
      z <- opSelect(op)
    } yield z
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

  private def performTrans(
      valCol: ColumnWithType,
      agg: AggOp): Try[ColumnWithType] = agg match {
    case AggFunction(n, inputs) =>
      for {
        cols <- sequence(inputs.map(Extraction.extractCol(valCol, _)))
        c <- SQLFunctionsExtraction.build(n, cols)
      } yield {
        c
      }
    case AggStruct(fields) =>
      sequence(fields.map { f =>
        performTrans(valCol, f.op).map(_.alias(f.fieldName))
      }).flatMap(ColumnWithType.struct(_ : _*))
  }
}

