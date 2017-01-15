package org.krapsh.ops

import com.typesafe.scalalogging.slf4j.{StrictLogging => Logging}
import org.apache.spark.sql.{Column, KrapshStubs, _}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.krapsh.ColumnWithType
import org.krapsh.structures.{AugmentedDataType, IsStrict}

import scala.util.{Failure, Success, Try}


object SQLFunctionsExtraction extends Logging {
  type SQLFunctionName = String

  def build(n: SQLFunctionName, inputs: Seq[ColumnWithType]): Try[ColumnWithType] = {
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

  private def rectifyNumericalType(inputs: Seq[ColumnWithType], c: Column): ColumnWithType = {
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
    (i, o) match {
      case (Some(it), Some(ot)) if it != ot =>
        ColumnWithType(c.cast(it.dataType), it, ref)
      case _ =>
        ColumnWithType(c, sadt, ref)
    }
  }
}
