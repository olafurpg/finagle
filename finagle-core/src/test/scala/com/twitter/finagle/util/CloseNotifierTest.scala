package com.twitter.finagle.util

import com.twitter.util.Promise
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import strawman.collection.immutable.{ List, Nil, Range }

@RunWith(classOf[JUnitRunner])
class CloseNotifierTest extends FunSuite {

  test("CloseNotifier should invoke onClose handlers in reverse order of adding") {
    val closing = new Promise[Unit]
    val notifier = CloseNotifier.makeLifo(closing)
    var invocations: List[Int] = Nil

    Range.inclusive(1, 10).foreach {
      i =>
        notifier.onClose {
          invocations ::= i
        }
    }

    closing.setDone()
    assert(invocations == Range.inclusive(1, 10).toList)
  }

  test("CloseNotifier should invoke onClose handler immediately if close event already happened") {
    val closing = new Promise[Unit]
    val notifier = CloseNotifier.makeLifo(closing)

    closing.setDone()
    var invoked = false
    notifier.onClose {
      invoked = true
    }

    assert(invoked)
  }
}
