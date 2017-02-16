package org.krapsh.structures

import org.apache.spark.sql.Row
import org.krapsh._
import spray.json.DefaultJsonProtocol._
import spray.json.{JsArray, JsBoolean, JsNull, JsNumber, JsString, JsValue}

case class UntypedNodeJson(
    locality: String,
    name: String,
    op: String,
    parents: Seq[String],
    logicalDependencies: Seq[String],
    extra: JsValue,
    _type: JsValue) {
  def ppString: String = {
    val ps = parents.map(p => "\n    - " + p).mkString("")
    val deps = logicalDependencies.map(p => "\n    - " + p).mkString("")
    s"""{
       |  name: $name
       |  op: $op
       |  parents:$ps
       |  dependencies:$deps
       |  extra:$extra
       |  (type):${_type}
       |  (locality): $locality
       |}
     """.stripMargin
  }
}

case class ComputationResultJson(
    status: String, // scheduled, running, finished_success, finished_failure
    finalError: Option[String], // TODO: better formatting
    // TODO: use cellWithType instead
    finalResult: JsValue) extends Serializable

object ComputationResultJson {
  def convert(x: Any): JsValue = x match {
    case null => JsNull
    case r: Row => convert(r.toSeq)
    case seq: Seq[_] => JsArray(seq.map(convert): _*)
    case i: Int => JsNumber(i)
    case i: java.lang.Integer => JsNumber(i)
    case l: java.lang.Long => JsNumber(l)
    case d: Double => JsNumber(d)
    case d: java.lang.Double => JsNumber(d)
    case b: Boolean => JsBoolean(b)
    case s: String => JsString(s)
    case x: Any => throw new Exception(s"Match error type=${x.getClass} val=$x")
  }

  implicit val computationResultJsonFormatter = jsonFormat3(ComputationResultJson.apply)

  val empty = ComputationResultJson(null, None, JsNull)

  def fromResult(status: ComputationResult): ComputationResultJson = status match {
    case ComputationScheduled =>
      empty.copy(status="scheduled")
    case ComputationRunning =>
      empty.copy(status="running")
    case ComputationDone(row) =>
      empty.copy(status="finished_success", finalResult = convert(row))
    case ComputationFailed(e) =>
      empty.copy(status="finished_failure", finalError = Some(e.getLocalizedMessage))
  }
}


object UntypedNodeJson2 {
  def pprint(s: Seq[UntypedNodeJson]): String = s.map(_.ppString).mkString("\n")
}
