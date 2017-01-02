package org.krapsh

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import com.typesafe.scalalogging.slf4j.{StrictLogging => Logging}
import spray.json.JsValue

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._

import org.krapsh.structures.{AugmentedDataType, CellWithType, IsStrict, UntypedNodeJson}

/**
 * A dataframe, along with the type as Krapsh wants to see it.
 *
 * Krapsh and Spark differ in their handling of nullability and primitive types:
 *  - krapsh allows primitive types as top-level, while Spark only accepts structures at the top
 *  level
 *  - Spark does not correctly handle nullability in a number of common situations, so the exact
 *  type has to be overridden by providing the correct data type.
 *
 * The only piece of information that does not get carried along is the metadata. It is ignored
 * for now.
 *
 * @param df the dataframe as manipulated by Spark
 * @param rectifiedSchema the type of the dataframe, as seen by Krapsh
 */
case class DataFrameWithType(df: DataFrame, rectifiedSchema: AugmentedDataType)

object DataFrameWithType extends Logging {
  def create(df: DataFrame): DataFrameWithType = {
    DataFrameWithType(df, AugmentedDataType(df.schema, IsStrict))
  }

  def asColumn(adf: DataFrameWithType): Column = {
    if (adf.rectifiedSchema.isPrimitive) {
      // There should be a single column in this dataframe.
      adf.df.schema.fields match {
        case Array(f) => adf.df.col(f.name)
        case _ => throw new Exception(s"Expected single field in $adf")
      }
    } else {
      asWrappedColumn(adf)
    }
  }

  /**
   * Unconditionnally wraps the content of the augmented dataframe into a structure. It does not
   * attempt to unpack single fields.
   */
  def asWrappedColumn(adf : DataFrameWithType): Column = {
    // Put everything in a struct, because this is the original type.
    logger.debug(s"asColumn: adf=$adf ")
    val colNames = adf.df.schema.fieldNames.toSeq
    logger.debug(s"asColumn: colNames=$colNames")
    val cols = colNames.map(n => adf.df.col(n))
    logger.debug(s"asColumn: cols=$cols")
    struct(cols: _*)

  }
}

// The result of an execution item.
// It is either a dataframe (for distributed transforms) or a Row (that has been cached locally)
// for the local transforms.
sealed trait ExecutionOutput
case class LocalExecOutput(row: CellWithType) extends ExecutionOutput
case class DisExecutionOutput(df: DataFrameWithType) extends ExecutionOutput


trait OpBuilder {
  def op: String
  def build(
      parents: Seq[ExecutionOutput],
      extra: JsValue,
      session: SparkSession): DataFrameWithType
}

class Registry extends Logging {

  private var ops: Map[String, OpBuilder] = Map.empty

  lazy val sparkSession: SparkSession = {
    logger.debug(s"Connecting registry $this to a Spark session")
    val s = SparkSession.builder().getOrCreate()
    logger.debug(s"Registry $this associated to Spark session $s")
    s
  }

  def addOp(builder: OpBuilder): Unit = synchronized {
    logger.debug(s"Registering ${builder.op}")
    ops += builder.op -> builder
  }

  private class SessionBuilder(
      raw: Seq[UntypedNodeJson],
      sessionId: SessionId,
      computationId: ComputationId,
      cache: () => ResultCache) extends Logging {

    def getItem(
        raw: UntypedNodeJson,
        done: Map[String, ExecutionItem]): ExecutionItem = {
      val parents = raw.parents.map { path =>
        done.getOrElse(path, throw new Exception(s"Missing $path"))
      }
      val logicalDependencies = raw.logicalDependencies.map { path =>
        done.getOrElse(path, throw new Exception(s"Missing $path"))
      }
      val b = ops.getOrElse(raw.op, throw new Exception(s"Operation ${raw.op} not registered"))
      val locality = raw.locality match {
        case "local" => Local
        case "distributed" => Distributed
      }
      val p = {
        val s = raw.name.split("/")
        require(s.size >= 1, s)
        if (s.head == computationId.repr) {
          Path(s.tail)
        } else {
          Path(s)
        }
      }
      val path = GlobalPath(sessionId, computationId, p)
      new ExecutionItem(parents, logicalDependencies, locality, path, cache, b, raw, sparkSession)
    }

    def getItems(
        todo: Seq[UntypedNodeJson],
        done: Map[String, ExecutionItem],
        doneInOrder: Seq[ExecutionItem]): Seq[ExecutionItem] = {
      if (todo.isEmpty) {
        return doneInOrder
      }
      // Find all the elements for which all the dependencies have been resolved:
      val (now, later) = todo.partition { raw =>
        (raw.parents ++ raw.logicalDependencies).forall(done.contains)
      }
      require(now.nonEmpty, (todo, done))
      val processed = now.map(raw => raw.name -> getItem(raw, done))
      val done2 = done ++ processed
      getItems(later, done2, doneInOrder ++ processed.map(_._2))
    }
  }

  /**
   * The items are guaranteed to be returned in topological order.
   *
   * This is not required for the raw elements.
   */
  def getItems(
      raw: Seq[UntypedNodeJson],
      sessionId: SessionId,
      computationId: ComputationId,
      cache: () => ResultCache): Seq[ExecutionItem] = {
    val sb = new SessionBuilder(raw, sessionId, computationId, cache)
    sb.getItems(raw, Map.empty, Seq.empty)
  }
}

object GlobalRegistry extends Logging {
  val registry: Registry = new Registry()

  def createBuilder(opName: String)(
      fun:(Seq[ExecutionOutput], JsValue) => DataFrameWithType): OpBuilder = {
    new OpBuilder {
      override def op = opName
      override def build(
          p: Seq[ExecutionOutput],
          ex: JsValue,
          session: SparkSession): DataFrameWithType = {
        fun(p, ex)
      }
    }
  }

  def createBuilderSession(opName: String)(
    fun:(Seq[ExecutionOutput], JsValue, SparkSession) => DataFrameWithType): OpBuilder = {
    new OpBuilder {
      override def op = opName
      override def build(
          p: Seq[ExecutionOutput],
          ex: JsValue,
          session: SparkSession): DataFrameWithType = {
        fun(p, ex, session)
      }
    }
  }

  // A builder that takes no argument other than some extra JSON input.
  def createTypedBuilder0(opName: String)(fun: JsValue => DataFrameWithType): OpBuilder = {
    def fun1(items: Seq[ExecutionOutput], jsValue: JsValue): DataFrameWithType = {
      require(items.isEmpty, items)
      fun(jsValue)
    }
    createBuilder(opName)(fun1)
  }

  // Builder that takes a single dataframe at the input.
  def createBuilderD(opName: String)(fun: (DataFrame, JsValue) => DataFrame): OpBuilder = {
    def fun1(items: Seq[ExecutionOutput], jsValue: JsValue): DataFrameWithType = {
      items match {
        case Seq(DisExecutionOutput(adf)) => DataFrameWithType.create(fun(adf.df, jsValue))
        case _ => throw new Exception(s"Unexpected input for op $opName: $items")
      }
    }
    createBuilder(opName)(fun1)
  }

  def createBuilderDD(opName: String)
                     (fun: (DataFrame, DataFrame, JsValue) => DataFrame): OpBuilder = {
    def fun1(items: Seq[ExecutionOutput], jsValue: JsValue): DataFrameWithType = {
      items match {
        case Seq(DisExecutionOutput(adf1), DisExecutionOutput(adf2)) =>
          DataFrameWithType.create(fun(adf1.df, adf2.df, jsValue))
        case _ => throw new Exception(s"Unexpected input for op $opName: $items")
      }
    }
    createBuilder(opName)(fun1)
  }

  def createTypedBuilderD(opName: String)(
        fun: (DataFrameWithType, JsValue) => DataFrameWithType): OpBuilder = {
    def fun1(items: Seq[ExecutionOutput], jsValue: JsValue): DataFrameWithType = {
      items match {
        case Seq(DisExecutionOutput(adf)) => fun(adf, jsValue)
        case _ => throw new Exception(s"Unexpected input for op $opName: $items")
      }
    }
    createBuilder(opName)(fun1)
  }
  /**
   * Attempts to extract some data content from a row.
   * For now, this only supports scalar values.
   */
  @throws[Exception]("when the specification is not respected")
  def extract[T: ClassTag](tcell: CellWithType): T = {
    tcell.cellType match {
      case x: T => x
      case x: Any =>
        throw new Exception(
          s"Cannot convert $x (${x.getClass}) to type ${implicitly[ClassTag[T]]}")
    }
  }

  def createTypedLocalBuilder_2_1[I1: ClassTag, I2: ClassTag, O: Encoder](
      opName: String)(fun: Function2[I1, I2, O]): OpBuilder = {
    def fun1(
        items: Seq[ExecutionOutput],
        js: JsValue,
        session: SparkSession): DataFrameWithType = {
      items match {
        case Seq(LocalExecOutput(tcell1), LocalExecOutput(tcell2)) =>
          val i1 = extract[I1](tcell1)
          val i2 = extract[I2](tcell2)
          val res = fun(i1, i2)
          DataFrameWithType.create(session.createDataset(Seq(res)).toDF())
      }
    }
    createBuilderSession(opName)(fun1)
  }

  def createLocalBuilder2(opName: String)(fun: (Column, Column) => Column): OpBuilder = {
    def fun1(items: Seq[ExecutionOutput], js: JsValue, session: SparkSession): DataFrameWithType = {
      val df = buildLocalDF(items, 2, session)
      val c1 = df.col("_1")
      val c2 = df.col("_2")
      buildLocalDF2(df, fun(c1, c2))
    }
    createBuilderSession(opName)(fun1)
  }

  def createLocalBuilder1(opName: String)(fun: Column => Column): OpBuilder = {
    def fun1(items: Seq[ExecutionOutput], js: JsValue, session: SparkSession): DataFrameWithType = {
      val df = buildLocalDF(items, 1, session)
      val c1 = df.col("_1")
      buildLocalDF2(df, fun(c1))
    }
    createBuilderSession(opName)(fun1)
  }

  private def buildLocalDF(items: Seq[ExecutionOutput], expectedNum: Int, session: SparkSession): DataFrame = {
    require(items.size == expectedNum, s"Expected $expectedNum elts, but got $items")
    val tcells = items.map {
      case LocalExecOutput(tcell) => tcell
      case x => throw new Exception(s"Expected a local output, got $x")
    }
    val fs = tcells.zipWithIndex.map { case (tcell, idx) =>
      logger.debug(s"idx=$idx tcell=$tcell rowType=${tcell.rowType}")
      tcell.rowType match {
        case StructType(Array(f)) => f.copy(name = s"_${idx + 1}")
        case x => throw new Exception(s"expected single field, got $tcell")
      }
    }
    val s = StructType(fs)
    logger.debug(s"createLocalBuilder2: s=$s")
    val rdata = tcells.map(_.cellData)
    val r = Row(rdata: _*)
    logger.debug(s"createLocalBuilder2: r=$r")
    val df = session.createDataFrame(Seq(r).asJava, s)
    logger.debug(s"createLocalBuilder2: df=$df")
    df
  }

  private def buildLocalDF2(df: DataFrame, c: Column): DataFrameWithType = {
    val dfout = df.select(c)
    logger.debug(s"createLocalBuilder2: dfout=$dfout")
    // TODO: all below is just for debugging
    val res = dfout.collect()
    val out = res match {
      case Array(Row(x)) =>
      case _ => throw new Exception(s"Expected single row in $res")
    }
    DataFrameWithType.create(dfout)
  }
}

