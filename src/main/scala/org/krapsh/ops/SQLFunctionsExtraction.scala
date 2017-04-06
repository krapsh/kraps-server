package org.krapsh.ops

import com.typesafe.scalalogging.slf4j.{StrictLogging => Logging}
import org.apache.spark.sql.{Column, KrapshStubs, _}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.krapsh.{ColumnWithType, KrapshException}
import org.krapsh.structures.{AugmentedDataType, IsNullable, IsStrict, Nullable}

import scala.util.{Failure, Success, Try}


object SQLFunctionsExtraction extends Logging {
  type SQLFunctionName = String

  def buildFunction(
      funName: String,
      inputs: Seq[ColumnWithType],
      ref: DataFrame): Try[ColumnWithType] = {
    FunctionRegistry.builtin.lookupFunctionBuilder(funName.toLowerCase) match {
      case Some(builder) =>
        val exps = inputs.map(_.col).map(KrapshStubs.getExpression)
        val expt = Try {
          builder.apply(exps)
        }
        expt.map { exp =>
          logger.debug(s"buildFunction: exp=$exp")
          val c = exp match {
            case agg: AggregateFunction =>
              KrapshStubs.makeColumn(agg.toAggregateExpression(isDistinct = false))
            case x =>
              KrapshStubs.makeColumn(x)
          }
          // As an approximation, the types are going to be the ones from Spark.
          // We may loose some information here, but it should be good enough as a start.
          // In order to determine the nullability, we need to apply the expression first.

          val adt = checkNullability(c, ref)
          val cwt = ColumnWithType(c, adt, ref)
          logger.debug(s"buildFunction: cwt=$cwt")
          // The output type may not be supported by Karps, so it needs to be rectified.
          val rectified = rectifyNumericalType(cwt)
          logger.debug(s"buildFunction: rectified=$rectified")
          rectified
        }

      case None => Failure(new KrapshException(s"Could not find function name '$funName' in the " +
        s"spark registry"))
    }
  }

  private def checkNullability(c: Column, df: DataFrame): AugmentedDataType = {
    val df2 = df.select(c)
    df2.schema.fields match {
      case Array(f1) =>
        AugmentedDataType.fromField(f1)
      case _ => KrapshException.fail(s"df=$df df2=$df2 c=$c")
    }
  }

  private def build(n: SQLFunctionName, inputs: Seq[ColumnWithType]): Try[ColumnWithType] = {
    logger.info(s"build called with n=$n inputs=$inputs")
    if (inputs.isEmpty) {
      // TODO: fix eventually
      return Failure(new Exception(s"Cannot currently build expression with no input: $n"))
    }
    (FunctionRegistry.expressions.get(n.toLowerCase) match {
      case None => Failure(new Exception(s"Cannot find $n in the set of builtin functions"))
      case Some((info, builder)) =>
        val e = builder.apply(inputs.map(i => KrapshStubs.getExpression(i.col)))
        e match {
          case agg: AggregateFunction =>
            Success(new Column(agg.toAggregateExpression(isDistinct = false)))
          case x =>
            Success(new Column(x))
        }
    }).map { c =>
      val c2 = rectifyNumericalType(inputs, c)
      logger.debug(s"build: $c -> $c2: $c->$c2")
      c2
    }
  }

  // The datatype of the column as seen by Spark.
  private def innerDataType(col: Column, ref: DataFrame): AugmentedDataType = {
    ref.select(col).schema.fields match {
      case Array(f) => AugmentedDataType.fromField(f)
      case x => throw new Exception(s"Expected one field, got $x. Input was $col")
    }
  }

  // This is very simple and does not attempt to be recursive.
  // TODO: make it recursive
  // TODO: prevent it from creating bad types to begin with.
  private def rectifyNumericalType(cwt: ColumnWithType): ColumnWithType = {
    cwt.rectifiedSchema.dataType match {
      case x: LongType =>
        val adt = cwt.rectifiedSchema.copy(dataType = IntegerType)
        ColumnWithType(cwt.col.cast(IntegerType), adt, cwt.ref)
      case _ => cwt
    }
  }

  private def rectifyNumericalType(inputs: Seq[ColumnWithType], c: Column): ColumnWithType = {
    logger.debug(s"rectifyNumericalType: inputs=$inputs")
    logger.debug(s"rectifyNumericalType: c=$c")
    // TODO: unsafe
    val ref = inputs.head.ref
    def isPrimNum(col: ColumnWithType): Option[AugmentedDataType] = {
      if (col.rectifiedSchema.nullability == IsStrict) {
        col.rectifiedSchema.dataType match {
          case x: LongType => Some(col.rectifiedSchema)
          case x: IntegerType => Some(col.rectifiedSchema)
          case _ => None
        }
      } else None
    }
    val i = inputs.map(isPrimNum) match {
      case Seq(Some(t)) => Some(t)
      case _ => None
    }
    val sadt = innerDataType(c, ref)
    // For the purpose of matching, since Spark makes some mistakes, we assume it is strict for
    // numerical stuff.
    val o = isPrimNum(ColumnWithType(c, AugmentedDataType(sadt.dataType, IsStrict), ref))
    val res = (i, o) match {
      case (Some(it), Some(ot)) if it != ot =>
        ColumnWithType(c.cast(it.dataType), it, ref)
      case _ =>
        ColumnWithType(c, sadt, ref)
    }
    logger.debug(s"rectifyNumericalType: res=$res")
    res
  }
}
