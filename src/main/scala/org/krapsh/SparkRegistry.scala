package org.krapsh

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}
import com.typesafe.scalalogging.slf4j.{StrictLogging => Logging}
import spray.json.{JsArray, JsObject, JsString, JsValue}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.krapsh.row.{AlgebraicRow, RowArray, RowCell}
import org.krapsh.structures._


object SparkRegistry extends Logging {
  import GlobalRegistry._

  object ImplicitAccessor extends SQLImplicits {
    override def _sqlContext: SQLContext =
      throw new Exception(s"SQL context is not available here")
    // TODO we could access the default SQL context from the default session?
  }


  def orderRowElements(df: DataFrame): DataFrame = {
    def fun(r: Row, st: StructType, at: ArrayType): Any = {
      val ar2 = AlgebraicRow.fromRow(r, st) match {
        case Success(AlgebraicRow(Seq(RowArray(seq)))) =>
          println(s">>orderRowElements: seq=$seq")
          val s = seq.sorted(AlgebraicRow.CellOrdering)
          println(s">>orderRowElements: s=$s")
          RowArray(s)
        case e =>
          throw new Exception(s"Could not convert $r of type $st: $e")
      }
      val arr = AlgebraicRow.toAny(ar2)
      println(s">>orderRowElements: arr=$arr")
      arr
    }
    val schema = df.schema
    val (fname, at) = schema.fields match {
      case Array(StructField(name, dt: ArrayType, _, _)) =>
        name -> dt
      case x => throw new Exception(s"Expected one field, got $df")
    }
    def u2(r: Row) = fun(r, schema, at)
    val localUdf = org.apache.spark.sql.functions.udf(u2 _, at)
    import df.sparkSession.implicits._
    val res = df.select(localUdf(struct(df.col(fname))))
    logger.debug(s"orderRowElements: df=$df, res=$res")
    res
  }

  def unpackRowElements(df: DataFrame): DataFrame = {
    def fun(r: Row, st: StructType): Any = {
      val ar2 = AlgebraicRow.fromRow(r, st) match {
        case Success(AlgebraicRow(Seq(RowArray(seq)))) =>
          val seq2 = seq.map {
            case RowCell(AlgebraicRow(Seq(cell))) => cell
            case x => throw new Exception(s"Expected a RowArray with a single element, got $x")
          }
          println(s">>unpackRowElements: seq=$seq")
          println(s">>unpackRowElements: seq2=$seq2")
          RowArray(seq2)
        case e =>
          throw new Exception(s"Could not convert $r of type $st: $e")
      }
      val arr = AlgebraicRow.toAny(ar2)
      println(s">>unpackRowElements: arr=$arr")
      arr
    }
    val schema = df.schema
    // We expect a single field with a an array that contains a struct with a single field as well.
    val (fname, at) = schema.fields match {
      case Array(StructField(name, ArrayType(StructType(Array(f)), n), _, _)) =>
        name -> ArrayType(f.dataType, containsNull = n)
      case x => throw new Exception(s"Expected one field, got $df")
    }
    def u2(r: Row) = fun(r, schema)
    val localUdf = org.apache.spark.sql.functions.udf(u2 _, at)
    import df.sparkSession.implicits._
    val res = df.select(localUdf(struct(df.col(fname))))
    logger.debug(s"orderRowElements: df=$df, res=$res")
    res
  }

  val collect = createTypedBuilderD("org.spark.Collect") { (adf, _) =>
    val df2 = if (adf.rectifiedSchema.isNullable && adf.rectifiedSchema.isPrimitive) {
        logger.debug(s"collect: primitive+nullable")
        // Nullable and primitive, we must wrap the content (otherwise the null values get
        // discarded).
        val c0 = DataFrameWithType.asWrappedColumn(adf)
        val c = collect_list(c0)
        val coll = adf.df.groupBy().agg(c)
        unpackRowElements(coll)
    } else {
      // The other cases: wrap the content, but no need to extract the values after that.
      logger.debug(s"collect: wrapped")
      val c0 = DataFrameWithType.asColumn(adf)
      val c = collect_list(c0)
      adf.df.groupBy().agg(c)
    }
    logger.debug(s"collect: df2=$df2")
    // Ensure that the final elements are sorted
    val df3 = orderRowElements(df2)
    val schema = AugmentedDataType.wrapArray(adf.rectifiedSchema)
    logger.info(s"collect: input df: $adf ${adf.df.schema}")
    logger.info(s"collect: output df3: $df3 ${df3.schema}")
    logger.info(s"collect: output schema: $schema")
    DataFrameWithType(df3, schema)
  }

  val localConstant = createTypedBuilder0("org.spark.LocalConstant") { z =>
    val typedCell = LocalSparkConversion.deserializeLocal(z) match {
      case Success(ct) => ct
      case Failure(e) =>
        throw new Exception(s"Deserialization failed", e)
    }
    val session = SparkSession.builder().getOrCreate()
    val df = session.createDataFrame(Seq(typedCell.row), typedCell.rowType)
    DataFrameWithType(df, typedCell.cellType)
  }

  val constant = createTypedBuilder0("org.spark.Constant") { z =>
    val cellCol = DistributedSparkConversion.deserializeDistributed(z) match {
      case Success(cc) => cc
      case Failure(e) =>
        throw new Exception(s"Deserialization failed", e)
    }
    val session = SparkSession.builder().getOrCreate()
    logger.debug(s"constant: data=$cellCol")
    val df = session.createDataFrame(cellCol.normalizedData, cellCol.normalizedCellDataType)
    logger.debug(s"constant: created dataframe: df=$df cellDT=${cellCol.cellDataType}")
    DataFrameWithType(df, cellCol.cellDataType)
  }

  val count = createBuilderD("org.spark.Count") { (df, _) =>
    // For now, the count is integer.
    // TODO: make the count a bigint eventually
    val df1 = df.groupBy().count()
    df1.schema.fields match {
      case Array(f) =>
        df1.select(df1.col(f.name).cast(IntegerType))
      case x =>
        logger.warn(s"os.Count: could not cast result to integer in $df1")
        df1
    }
  }

  val persist = createBuilderD("org.spark.Persist") { (df, _) =>
    // For now, we just use the default storage level.
    df.persist()
    // Force the materialization of the cache, as multiple
    // calls to this cache may be issued after that.
    df.count()
    df
  }

  val cache = createBuilderD("org.spark.Cache") { (df, _) =>
    // For now, we just use the default storage level.
    df.persist()
    // Force the materialization of the cache, as multiple
    // calls to this cache may be issued after that.
    df.count()
    df
  }

  // This is a hack, it should be resolved into persist/unpersist
  val autocache = createBuilderD("org.spark.Autocache") { (df, _) =>
    // For now, we just use the default storage level.
    df.persist()
    // Force the materialization of the cache, as multiple
    // calls to this cache may be issued after that.
    df.count()
    df
  }

  val unpersist = createBuilderD("org.spark.Unpersist") { (df, _) =>
    // The call is blocking for now, for debugging purposes.
    df.unpersist(blocking = true)
    df
  }

  // TODO: remove and replace by unpersist
  val uncache = createBuilderD("org.spark.Uncache") { (df, _) =>
    // The call is blocking for now, for debugging purposes.
    df.unpersist(blocking = true)
    df
  }

  val identity = createBuilderD("org.spark.Identity") { (df, _) => df }

  val localIdentity = createLocalBuilder1("org.spark.LocalIdentity") {x => x}

  val localDiv = createLocalBuilder2("org.spark.LocalDiv") (_ / _)

  val localPlus = createLocalBuilder2("org.spark.LocalPlus") (_ + _)

  val localMult = createLocalBuilder2("org.spark.LocalMult") (_ * _)

  val localNegate = createLocalBuilder1("org.spark.LocalNegate") (- _)

  val localMinus = createLocalBuilder2("org.spark.LocalMinus") (_ - _)

  val localAbs = createLocalBuilder1("org.spark.LocalAbs") (abs)

  val localMax = createLocalBuilder2("org.spark.LocalMax") { (c1, c2) =>
    val c = c1.when(c1 <= c2, 0).otherwise(1)
    c * c1 + (- c + 1) * c2
  }

  val localMin = createLocalBuilder2("org.spark.LocalMin") { (c1, c2) =>
    val c = c1.when(c1 <= c2, 0).otherwise(1)
    c * c2 + (- c + 1) * c1
  }

  val select = createTypedBuilderD("org.spark.Select") { (adf, js) =>
    logger.debug(s"select: adf=$adf js=$js")
    val (cols, adt) = SparkSelector.select(adf, js) match {
      case Success(z) => z
      case Failure(e) => throw new Exception(s"Failure when calling select", e)
    }
    logger.debug(s"select: cols = $cols")
    cols.foreach(_.explain(true))
    val df = adf.df.select(cols: _*)
    logger.debug(s"select: df=$df, adt=$adt")
    df.printSchema()
    DataFrameWithType(df, adt)
  }

  val join = createBuilderDD("org.spark.Join") { (df1, df2, js) =>
    val key1f = df1.schema.fields match {
      case Array(keyf, _) => keyf
      case x => throw new Exception(s"The schema should be a (key, val), but got $x")
    }
    val key2f = df2.schema.fields match {
      case Array(keyf, _) => keyf
      case x => throw new Exception(s"The schema should be a (key, val), but got $x")
    }
    require(key1f == key2f,
      s"The two dataframe keys are not compatible: $key1f in $df1 ... $key2f in $df2")
    val joinType = js match {
      case JsString(s) =>
        require(List("inner").contains(s), s"Unknown join type: $s")
        s
      case _ => throw new Exception(s"Unknown join data type: $js")
    }

    val res = df1.join(df2, usingColumns = Seq(key1f.name), joinType = joinType)
    res
  }

  val sum = createBuilderD("org.spark.Sum") { (df, js) =>
    // Spark decides sometimes to perform some automated casting.
    // This does not go well with strong typing, so it is reversed here.
    // TODO() add a check for overflows
    val df1 = df.groupBy().sum()

    (df.schema.fields, df1.schema.fields) match {
      case (Array(f), Array(f1)) if f1.dataType != f.dataType =>
        // Spark has decided to broaden the final data type. We coerce it back.
        val df2 = df1.select(df1.col(f1.name).cast(f.dataType))
        logger.debug(s"os.Sum: forcing cast of output: $df1 -> $df2")
        df2
      case (Array(f), Array(f1)) if f1.dataType == f.dataType =>
        // No change
        df1
      case _ =>
        // Not sure what should happen here. Just log it and return the
        // Spark result. It may be wrong.
        logger.debug(s"os.Sum: forcing cast of output: df1=$df1 df=$df")
        df1
    }
  }

  val all = Seq(
    autocache,
    cache,
    collect,
    constant,
    count,
    identity,
    join,
    localAbs,
    localConstant,
    localDiv,
    localIdentity,
    localMax,
    localMin,
    localMinus,
    localMult,
    localNegate,
    localPlus,
    persist,
    select,
    sum,
    uncache,
    unpersist)

  def setup(): Unit = {
    all.foreach(registry.addOp)
  }
}

object SparkSelector extends Logging {

  import org.krapsh.structures.JsonSparkConversions.{getString, get, sequence, getStringList}


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
      case InnerOp(ColExtraction(Seq())) =>
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
            c.as(f.fieldName) -> StructField(f.fieldName, adt2.dataType, adt2.isNullable)
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
  private case class ColExtraction(path: List[String]) extends ColOp
  //private case class ColFunction(name: String, inputs: Seq[ColOp]) extends ColOp

  private case class Field(fieldName: String, fieldTrans: StructuredTransform)

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
          l <- getStringList(js.fields, "field")
        } yield {
          ColExtraction(l)
        }
      case s: String =>
        Failure(new Exception(s"Cannot understand $s in $js"))
    }

    for {
      op <- getString(js.fields, "colOp")
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
        Field(fName, trans)
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
          c.as(f.fieldName) -> StructField(f.fieldName, adt2.dataType, adt2.isNullable)
        }
      })
      for (fs <- fst) yield {
        val st = StructType(fs.map(_._2))
        val str = struct(fs.map(_._1): _*)
        str -> AugmentedDataType(st, IsStrict)
      }
  }


  private def extractType(adt: AugmentedDataType, path: List[String]): Try[AugmentedDataType] = {
    (path, adt) match {
      case (Nil, _) =>
        Success(adt)
      case (h :: t, AugmentedDataType(st: StructType, nullability)) =>
        // Look into a sub field.
        val res = st.fields.find(_.name == h) match {
          case None => Failure(new Exception(s"Cannot find field $h in $st"))

          case Some(StructField(_, st2: StructType, nullable, _)) if nullable =>
            // We can go deeper in the structure, but we mark the result as nullable.
            val adt2 = AugmentedDataType(st2, IsNullable)
            extractType(adt2, t)
          case Some(StructField(_, st2: StructType, _, _)) =>
            // We can go deeper in the structure.
            // The nullability will depend on the underlying structure.
            val adt2 = AugmentedDataType(st2, IsNullable)
            extractType(adt2, t)
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

  private def extractCol(adf: DataFrameWithType, path: List[String]): Column = {
    // We are asked all the dataframe, but the schema is a primitive: go straight for the
    // unique column of this dataframe.
    logger.debug(s"extractCol: path=$path adf=$adf")
    (path, adf.rectifiedSchema.topLevelStruct) match {
      case (Nil, None) =>
        val n = adf.df.schema.fields match {
          case Array(f) => f.name
          case x => throw new Exception(s"Programming error: $x $path $adf")
        }
        adf.df.col(n)
      case (fname :: t, Some(st)) =>
        // Because of a bug somewhere, the first layer has to be unrolled for simple
        // projects. It is unclear to me why packing as a struct first and then accessing
        // the schema does not work.
        // We are supposed to have checked already that the path and the types are compatible.
        extractCol(adf.df.col(fname), t)
      case (Nil, Some(st)) =>
        logger.warn(s"Path will not work: $path and $adf")
        DataFrameWithType.asColumn(adf)
      case _ =>
        throw new Exception(s"extractCol: Invalid combination $path and $adf")
    }
  }

  // This is not checked, because the check is done when finding the type of the element.
  private def extractCol(col: Column, path: List[String]): Column = path match {
    case Seq() => col
    case h :: t =>
      extractCol(col.getField(h), t)
  }
}
