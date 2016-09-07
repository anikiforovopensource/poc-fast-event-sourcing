/*
 * Copyright (c) 2016, PagerDuty
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions
 * and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of
 * conditions and the following disclaimer in the documentation and/or other materials provided with
 * the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.pagerduty.overcharged

import akka.actor._
import com.pagerduty.eris.TimeUuid
import com.typesafe.config._
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}
import scala.concurrent.duration._

object GlobalSettings {
  val CassHost = "localhost"
  val CassPort = 9160

  val PartitionCount = 10
  val UserCount = 100
  val WriteCache = 40
  val RemoveCache = 40
  val WriteDelay = 50.millis
  val RemoveDelay = 50.millis
  val SnapshotInterval = 100
}

object Main {
  def main(args: Array[String]): Unit = {
    val simulation = new Simulation()
    simulation.run()
    System.exit(0)
  }
}

object CounterSettings {
  val FrequentCountersEnabled = true
  val DaoCountersEnabled = true
}
class GlobalCounter(val enabled: Boolean) {
  private val counter = new AtomicInteger(0)
  def bump(): Unit = {
    if (enabled) counter.incrementAndGet()
  }
  def getAndReset = counter.getAndSet(0)
}

class GlobalMax(val enabled: Boolean) {
  private val currentMax = new AtomicInteger(0)
  def max(n: Int): Unit = {
    if (!enabled) return

    var cur = currentMax.get
    while (n > cur && !currentMax.compareAndSet(cur, n)) {
      cur = currentMax.get
    }
  }
  def getAndReset = currentMax.getAndSet(0)
}

class GlobalAverage(val enabled: Boolean) {
  private val total = new AtomicLong(0)
  private val counts = new AtomicInteger(0)
  def accum(n: Int): Unit = {
    if (!enabled) return

    var cur = total.get
    while (!total.compareAndSet(cur, cur + n)) {
      cur = total.get
    }
    counts.incrementAndGet()
  }
  def getAndReset: Double = {
    val t = total.getAndSet(0)
    val c = counts.getAndSet(0)
    t / c.toDouble
  }
}

object ProcessedCounter extends GlobalCounter(CounterSettings.FrequentCountersEnabled)
object SendCounter extends GlobalCounter(CounterSettings.FrequentCountersEnabled)

object DaoWriteCounter extends GlobalCounter(CounterSettings.DaoCountersEnabled)
object DaoRemoveCounter extends GlobalCounter(CounterSettings.DaoCountersEnabled)
object DaoSnapshotCounter extends GlobalCounter(CounterSettings.DaoCountersEnabled)

object RoundTripMaxLatencyMs extends GlobalMax(CounterSettings.FrequentCountersEnabled)
object RoundTripAvgLatencyMs extends GlobalAverage(CounterSettings.FrequentCountersEnabled)


class User extends Actor {
  def receive = {
    case EntityQueue.GetSnapshot =>
      sender ! EntityQueue.SnapshotResult("no_state")

    case EntityQueue.ProcessEvent(event, _) =>
      sender ! EntityQueue.ReadyForNextEvent
      ProcessedCounter.bump()
  }
}

class Simulation {
  val persistenceCtx = PersistenceCtx.makeTestContext()
  val UserType = "user"

  val entityTypeFactories = Map(
    UserType -> ((context: ActorContext, entityId: EntityId) => {
      context.actorOf(Props(new User))
    })
  )
  val actorSystem = startActorSystem()

  def run(): Unit = {
    spam(partitions = GlobalSettings.PartitionCount, usersPerPartition = GlobalSettings.UserCount)
    for (i <- 0 until 15) samplerLoop()
  }

  def samplerLoop(): Unit = {
    val start = System.currentTimeMillis
    Thread.sleep(2000)
    val totalInSec = (System.currentTimeMillis - start)*1e-3
    val sendPerSecond = (SendCounter.getAndReset / totalInSec).toInt

    val daoWritePerSecond = (DaoWriteCounter.getAndReset / totalInSec).toInt
    val daoRemovePerSecond = (DaoRemoveCounter.getAndReset / totalInSec).toInt
    val daoSnapshotPerSecond = (DaoSnapshotCounter.getAndReset / totalInSec).toInt
    val cassOpsPerSecond = daoWritePerSecond + daoRemovePerSecond + daoSnapshotPerSecond

    val processPerSecond = (ProcessedCounter.getAndReset / totalInSec).toInt
    val logicalOpsPerSecond =
      if (CounterSettings.FrequentCountersEnabled) processPerSecond * 3 + daoSnapshotPerSecond
      else 0
    val maxRoundTripLatency = RoundTripMaxLatencyMs.getAndReset
    val avgRoundTripLatency = RoundTripAvgLatencyMs.getAndReset.toInt


    println(s"Per second: cassOps=$cassOpsPerSecond, logicalOps=$logicalOpsPerSecond, send=$sendPerSecond, " +
      s"process=$processPerSecond; Round trip millis: avg=$avgRoundTripLatency, max=$maxRoundTripLatency")
  }

  def startActorSystem() = {
    val config = ConfigFactory.load()
    ActorSystem("app", config)
  }

  class SpamerActor(userId: String, partitionManager: ActorRef) extends Actor {
    import context.dispatcher
    var messageId = 0
    var lastSentAt: Option[Long] = None

    def send(): Unit = {
      lastSentAt = Some(System.nanoTime())
      sendSimulatedMessage(userId, messageId, partitionManager, sender = self)
      messageId += 1
      SendCounter.bump()
    }
    send()

    def receive = {
      case _: EventQueue.EventRecorded =>
        updateRoundTripMax()
        send()
    }

    def updateRoundTripMax(): Unit = {
      lastSentAt.foreach { sentAt =>
        val delayNanos = System.nanoTime() - sentAt
        val delayMillis = (delayNanos/1000000).toInt
        RoundTripMaxLatencyMs.max(delayMillis)
        RoundTripAvgLatencyMs.accum(delayMillis)
      }
    }
  }

  def mkSpamer(userId: String, partitionManager: ActorRef): ActorRef = {
    actorSystem.actorOf(Props(new SpamerActor(userId, partitionManager)))
  }

  def spam(partitions: Int, usersPerPartition: Int): Unit = {
    for (p <- 0 until partitions) {
      val ctx = persistenceCtx
      val partitionPersistenceCtx = PartitionPersistenceCtx(
        p, ctx.pendingEventsBatchDao, ctx.completedEventsBatchDao, ctx.snapshotDao
      )
      val partitionManager = actorSystem.actorOf(
        PartitionManager.props(partitionPersistenceCtx, entityTypeFactories)
      )
      for (u <- 0 until usersPerPartition) {
        mkSpamer(s"${p}_$u", partitionManager)
      }
    }
  }

  def sendSimulatedMessage(userId: String, messageId: Int, partitionManager: ActorRef, sender: ActorRef): Unit = {
    val wrappedMessage = IncomingMessage(
      destination = Destination(UserType, s"user$userId"),
      requestId = TimeUuid(),
      eventData = s"SimulatedMessage$messageId"
    )
    partitionManager.tell(wrappedMessage, sender)
  }
}
