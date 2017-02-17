package org.krapsh

import scala.collection.mutable.ArrayBuffer
import scala.util.{Success, Failure}

import com.typesafe.scalalogging.slf4j.{StrictLogging => Logging}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import org.krapsh.structures.{CellWithType, UntypedNodeJson}


case class RDDId(repr: Int) extends AnyVal

/**
 * The elements that are going to be executed by the graph.
 */
class ExecutionItem(
    val dependencies: Seq[ExecutionItem],
    val logicalDependencies: Seq[ExecutionItem],
    val locality: Locality,
    val path: GlobalPath,
    cache: () => ResultCache,
    builder: OpBuilder,
    raw: UntypedNodeJson,
    session: SparkSession) extends Logging {

  private var _execId: Option[String] = None

  lazy val cacheAsUsed: ResultCache = cache()

  lazy val rectifiedDataFrameSchema = dataframeWithType.rectifiedSchema

  lazy val dataframe: DataFrame = dataframeWithType.df

  lazy val dataframeWithType: DataFrameWithType = {
    logger.debug(s"Creating dataframe for node: $path, results=${cache}")
    val outputs = dependencies.map { item =>

      cacheAsUsed.finalResult(item.path).map(LocalExecOutput.apply)
        .getOrElse(DisExecutionOutput(item.dataframeWithType))
    }
    logger.debug(s"Dependent outputs for node: $path: $outputs")
    builder.build(outputs, raw.extra, session)
  }

  lazy val encoderOut: ExpressionEncoder[Row] = {
    KrapshStubs.getBoundEncoder(dataframe)
  }

  lazy val queryExecution = dataframe.queryExecution
  lazy val executedPlan = queryExecution.executedPlan
  lazy val logical = queryExecution.logical

  lazy val rdd: RDD[InternalRow] = {
    // Get the execution id first.
    KrapshStubs.withNewExecutionId(session, queryExecution) {
      _execId = Option(session.sparkContext.getLocalProperty("spark.sql.execution.id"))
      require(_execId.isDefined)
    }

    KrapshStubs.withExecutionId(session.sparkContext, executionId) {
      executedPlan.execute()
    }
  }

  lazy val rddId = RDDId(rdd.id)

  lazy val collectedInternal: Seq[InternalRow] = {
    val results = ArrayBuffer[InternalRow]()
    KrapshStubs.withExecutionId(session.sparkContext, executionId) {
      rdd.collect().foreach(r => results.append(r.copy()))
    }
    results
  }

  lazy val collected: Seq[Row] = {
    collectedInternal.map(encoderOut.fromRow)
  }

  lazy val RDDDependencies: Seq[RDDId] = {
    captureRDDIds(rdd).toSeq.sortBy(_.repr)
  }

  def executionId: String = _execId.get

  private def captureRDDIds(rdd: RDD[_]): Set[RDDId] = {
    val deps = rdd.dependencies.map(_.rdd).flatMap(captureRDDIds)
    (deps :+ rddId).toSet
  }
}

object ExecutionItem {
  private case class SparkState()
}
