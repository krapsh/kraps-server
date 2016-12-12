package org.krapsh.ids

import scala.concurrent.Future
import scala.util.Try


//case class MyId (x: Int)

class MyId private (private val x: Int) extends AnyVal {
  def isLucian: Boolean = x == 0
  def largest(other: MyId): MyId = new MyId(math.max(x, other.x))
  def prettyString: String = x match {
    case 0 => "Lucian"
    case other => other.toString
  }
}

object MyId {
  def create(i: Int): Option[MyId] = {
    if (i >= 0) { Some(new MyId(i)) } else None
  }
}
//
//object MyIdTest {
//  def test(): Unit = {
//    val x = MyId.create(3).get
//    x.hashCode()
//    x.largest(x) == x
//    x == x
//  }
//}

object json {
  sealed trait JSValue
  case object JSNull extends JSValue
  case class JSBool(b: Boolean) extends JSValue


  def jsonToAny(js: JSValue): Any = js match {
    case JSNull => null
    case JSBool(b) => b
  }

  def toInt(s: String): Int = ???

  def toInt2(s: String): Option[Int] = ???

  def toInt3(s: String): Try[Int] = ???


}


object ZZZ {
  type UserName = Int
  type Webpassword = Int
  type AuthenticatedUser = Int

  def authenticate(name: String, password: String): Future[Boolean] = ???

  def authenticate2(name: UserName, password: Webpassword): Future[AuthenticatedUser] = ???
}

object MyIdWrapper {
  private case class Z(a: Int, b: MyId)
  class MyId2 private (private val z: Z) extends AnyVal {
    def b: MyId = z.b
  }

//  def create(i: Int, b: Int): Option[MyId2] = {
//    if (i > 0 && b >= 0) Some(new MyId2(i, b)) else None
//  }
}


