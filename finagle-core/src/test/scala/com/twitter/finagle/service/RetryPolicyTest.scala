package com.twitter.finagle.service

import RetryPolicy._
import com.twitter.conversions.time._
import com.twitter.finagle.{ChannelClosedException, Failure, TimeoutException, WriteException}
import com.twitter.util._
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner
import strawman.collection.immutable.{ #::, LazyList }

@RunWith(classOf[JUnitRunner])
class RetryPolicyTest extends FunSpec {
  def getBackoffs(
    policy: RetryPolicy[Try[Nothing]],
    exceptions: LazyList[Exception]
  ): LazyList[Duration] =
    exceptions match {
      case LazyList.Empty => LazyList.empty
      case e #:: tail =>
        policy(Throw(e)) match {
          case None => LazyList.empty
          case Some((backoff, p2)) => backoff #:: getBackoffs(p2, tail)
        }
    }

  describe("RetryPolicy") {
    val NoExceptions: PartialFunction[Try[Nothing], Boolean] = {
      case _ => false
    }
    val timeoutExc = new TimeoutException {
      protected val timeout = 0.seconds
      protected val explanation = "!"
    }

    it("should WriteExceptionsOnly") {
      val weo = WriteExceptionsOnly orElse NoExceptions

      assert(!weo(Throw(new Exception)))
      assert(weo(Throw(WriteException(new Exception))))
      assert(!weo(Throw(Failure(new Exception, Failure.Interrupted))))
      // it's important that this failure isn't retried, despite being "restartable".
      // interrupted futures should never be retried.
      assert(!weo(Throw(Failure(new Exception, Failure.Interrupted|Failure.Restartable))))
      assert(weo(Throw(Failure(new Exception, Failure.Restartable))))
      assert(!weo(Throw(Failure(new Exception, Failure.Rejected|Failure.NonRetryable))))
      assert(!weo(Throw(timeoutExc)))
    }

    it("should TimeoutAndWriteExceptionsOnly") {
      val taweo = TimeoutAndWriteExceptionsOnly orElse NoExceptions

      assert(!taweo(Throw(new Exception)))
      assert(taweo(Throw(WriteException(new Exception))))
      assert(!taweo(Throw(Failure(new Exception, Failure.Interrupted))))
      assert(taweo(Throw(Failure(timeoutExc, Failure.Interrupted))))
      assert(taweo(Throw(timeoutExc)))
      assert(taweo(Throw(new com.twitter.util.TimeoutException(""))))
    }

    it("RetryableWriteException matches retryable exception") {
      val retryable = Seq(Failure.rejected("test"), WriteException(new Exception))
      val nonRetryable =
        Seq(Failure("test", Failure.Interrupted), new Exception, new ChannelClosedException,
          Failure("boo", Failure.NonRetryable))

      retryable.foreach {
        case RetryPolicy.RetryableWriteException(_) =>
        case _ => fail("should match RetryableWriteException")
      }

      nonRetryable.foreach {
        case RetryPolicy.RetryableWriteException(_) =>
          fail("should not match RetryableWriteException")
        case _ =>
      }
    }
  }

  case class IException(i: Int) extends Exception

  val iExceptionsOnly: PartialFunction[Try[Nothing], Boolean] = {
    case Throw(IException(_)) => true
  }

  val iGreaterThan1: Try[Nothing] => Boolean = {
    case Throw(IException(i)) if i > 1 => true
    case _ => false
  }

  describe("RetryPolicy.filter/filterEach") {
    val backoffs = LazyList(10.milliseconds, 20.milliseconds, 30.milliseconds)
    val policy = RetryPolicy.backoff(backoffs)(iExceptionsOnly).filter(iGreaterThan1)

    it("returns None if filter rejects") {
      val actual = getBackoffs(policy, LazyList(IException(0), IException(1)))
      assert(actual == LazyList.empty)
    }

    it("returns underlying result if filter accepts first") {
      val actual = getBackoffs(policy, LazyList(IException(2), IException(0)))
      assert(actual == backoffs.take(2))
    }
  }

  describe("RetryPolicy.filterEach") {
    val backoffs = LazyList(10.milliseconds, 20.milliseconds, 30.milliseconds)
    val policy = RetryPolicy.backoff(backoffs)(iExceptionsOnly).filterEach(iGreaterThan1)

    it("returns None if filterEach rejects") {
      val actual = getBackoffs(policy, LazyList(IException(0), IException(1)))
      assert(actual == LazyList.empty)
    }

    it("returns underlying result if filterEach accepts") {
      val actual = getBackoffs(policy, LazyList(IException(2), IException(2), IException(0)))
      assert(actual == backoffs.take(2))
    }
  }

  describe("RetryPolicy.limit") {
    var currentMaxRetries: Int = 0
    val maxBackoffs = LazyList.fill(3)(10.milliseconds)
    val policy =
      RetryPolicy.backoff(maxBackoffs)(RetryPolicy.ChannelClosedExceptionsOnly)
        .limit(currentMaxRetries)

    it("limits retries dynamically") {
      for (i <- 0 until 5) {
        currentMaxRetries = i
        val backoffs = getBackoffs(policy, LazyList.fill(3)(new ChannelClosedException()))
        assert(backoffs == maxBackoffs.take(i min 3))
      }
    }
  }

  describe("RetryPolicy.combine") {
    val channelClosedBackoff = 10.milliseconds
    val writeExceptionBackoff = 0.milliseconds

    val combinedPolicy =
      RetryPolicy.combine(
        RetryPolicy.backoff(Backoff.const(Duration.Zero).take(2))(RetryPolicy.WriteExceptionsOnly),
        RetryPolicy.backoff(LazyList.fill(3)(channelClosedBackoff))(RetryPolicy.ChannelClosedExceptionsOnly)
      )

    it("return None for unmatched exception") {
      val backoffs = getBackoffs(combinedPolicy, LazyList(new UnsupportedOperationException))
      assert(backoffs == LazyList.empty)
    }

    it("mimicks first policy") {
      val backoffs = getBackoffs(combinedPolicy, LazyList.fill(4)(WriteException(new Exception)))
      assert(backoffs == LazyList.fill(2)(writeExceptionBackoff))
    }

    it("mimicks second policy") {
      val backoffs = getBackoffs(combinedPolicy, LazyList.fill(4)(new ChannelClosedException()))
      assert(backoffs == LazyList.fill(3)(channelClosedBackoff))
    }

    it("interleaves backoffs") {
      val exceptions = LazyList(
        new ChannelClosedException(),
        WriteException(new Exception),
        WriteException(new Exception),
        new ChannelClosedException(),
        WriteException(new Exception)
      )

      val backoffs = getBackoffs(combinedPolicy, exceptions)
      val expectedBackoffs = LazyList(
        channelClosedBackoff,
        writeExceptionBackoff,
        writeExceptionBackoff,
        channelClosedBackoff
      )
      assert(backoffs == expectedBackoffs)
    }
  }

  describe("RetryPolicy.Never") {
    val never = RetryPolicy.Never.asInstanceOf[RetryPolicy[Try[Int]]]
    it("should not retry") {
      assert(None == never(Return(1)))
      assert(None == never(Throw(new RuntimeException)))
    }
  }

  describe("RetryPolicy.none") {
    val nah = RetryPolicy.none
    it("should not retry") {
      assert(None == nah((1, Return(1))))
      assert(None == nah((1, Throw(new RuntimeException))))
    }
  }
}
