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
import akka.pattern._
import com.pagerduty.eris.TimeUuid
import scala.concurrent.duration._

case class EntityPersistenceCtx(
  partitionId: PartitionId,
  snapshotPersistence: ActorRef,
  pendingEventPersistence: ActorRef,
  completedEventPersistence: ActorRef
)

object SnapshotPersistence {
  case class PersistSnapshot(entityId: EntityId, snapshot: EntityData)
  case class SnapshotPersisted(entityId: EntityId)

  case class LoadSnapshot(entityId: EntityId)
  case class SnapshotLoaded(entityId: EntityId, snapshot: EntityData)

  def props(
    partitionPersistenceCtx: PartitionPersistenceCtx
  ): Props = {
    Props(new SnapshotPersistence(partitionPersistenceCtx))
  }
}

class SnapshotPersistence(
  partitionPersistenceCtx: PartitionPersistenceCtx
) extends Actor {
  import SnapshotPersistence._
  import context.dispatcher

  val snapshotDao = partitionPersistenceCtx.snapshotDao

  def receive = {
    case PersistSnapshot(entityId, snapshot) =>
      val replyTo = sender()
      snapshotDao.storeSnapshot(entityId, snapshot).map(_ => replyTo ! SnapshotPersisted(entityId))
  }
}

object EventPersistence {
  case class PersistEvent(event: XEvent)
  case class EventPersisted(eventId: XEventId)

  case class LoadEvents(entityId: EntityId, lastEventId: Option[XEventId], limit: Int)
  case class EventsLoaded(events: Seq[XEvent])

  case class RemoveEvents(entityId: EntityId, eventIds: Iterable[XEventId])

  case object PersistTimeout
  case object RemoveTimeout

  def props(
    name: String,
    partitionPersistenceCtx: PartitionPersistenceCtx
  ): Props = {
    Props(new EventPersistence(name, partitionPersistenceCtx))
  }
}

class EventPersistence(
  name: String,
  partitionPersistenceCtx: PartitionPersistenceCtx
) extends Actor {
  import EventPersistence._
  import context.dispatcher

  def partitionId = partitionPersistenceCtx.partitionId
  val eventDao = partitionPersistenceCtx.pendingEventsBatchDao
  var persistBatch = Set.empty[(ActorRef, XEvent)]
  var persistTimer: Option[Cancellable] = None
  var removeBatch = Set.empty[XEventId]
  var removeTimer: Option[Cancellable] = None

  def receive = {
    case LoadEvents(_, _, _) =>
      sender() ! EventsLoaded(Seq.empty)

    case PersistEvent(event) =>
      if (persistBatch.isEmpty) {
        persistTimer = Some(context.system.scheduler.scheduleOnce(GlobalSettings.WriteDelay, self, PersistTimeout))
      }
      persistBatch += (sender() -> event)

      if (persistBatch.size >= GlobalSettings.WriteCache) {
        persistAll()
      }

    case PersistTimeout =>
      persistAll()

    case RemoveEvents(entityId, eventIds) =>
      if (removeBatch.isEmpty) {
        removeTimer = Some(context.system.scheduler.scheduleOnce(GlobalSettings.RemoveDelay, self, RemoveTimeout))
      }
      removeBatch ++= eventIds

      if (removeBatch.size >= GlobalSettings.RemoveCache) {
        removeAll()
      }

    case RemoveTimeout =>
      removeAll()
  }

  def persistAll(): Unit = {
    val replyToMap = for ((replyTo, event) <- persistBatch) yield {
      replyTo -> event.eventId
    }
    val events = for ((_, event) <- persistBatch) yield {
      event
    }
    val rowId = s"stubRowId$partitionId"
    eventDao.persistEvents(rowId, events).map { _ =>
      for ((replyTo, eventId) <- replyToMap) {
        replyTo ! EventPersisted(eventId)
      }
    }
    persistBatch = Set.empty
    persistTimer.map(_.cancel())
    persistTimer = None
  }

  def removeAll(): Unit = {
    val rowId = s"stubRowId$partitionId"
    eventDao.removeEvents(rowId, removeBatch)
    removeBatch = Set.empty
    removeTimer.map(_.cancel())
    removeTimer = None
  }
}
