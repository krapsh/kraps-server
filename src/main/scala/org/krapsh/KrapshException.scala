package org.krapsh

/**
 * An internal exception caused by a programming error inside Krapsh.
 * @param cause some useful message
 */
class KrapshException(cause: String) extends Exception(cause) {
}

object KrapshException {
  @throws[KrapshException]("always")
  def fail(msg: String, cause: Exception = null): Nothing = {
    throw new KrapshException(msg)
  }
}
