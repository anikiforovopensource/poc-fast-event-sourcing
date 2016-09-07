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

object EventQueue {
  case class PutNextEvent(event: XEvent)
  case class EventRecorded(requestId: TimeUuid)
  case object GetNextEvent
  case class NextEvent(event: XEvent)
  case class ForgetEvent(event: XEvent)

  trait State
  case object FetchingPendingEvents extends State
  case object Ready extends State
  case object WaitingForWriteToComplete extends State

  trait Data
  case object NoData extends Data
  case class Write(sender: ActorRef, write: XEvent) extends Data
  case class AllData(cache: IndexedSeq[XEvent], write: Option[Write]) extends Data

  def props(
    entityId: EntityId,
    entityPersistenceCtx: EntityPersistenceCtx
  ): Props = {
    Props(new EventQueue(entityId, entityPersistenceCtx))
  }
}

class EventQueue(
  entityId: EntityId,
  entityPersistenceCtx: EntityPersistenceCtx
) extends ExtendedLoggingFSM[EventQueue.State, EventQueue.Data] with Stash {
  import EventQueue._

  val pendingEventsPersistence = entityPersistenceCtx.pendingEventPersistence
  pendingEventsPersistence ! EventPersistence.LoadEvents(entityId, None, 1000)

  var transientEvents = IndexedSeq.empty[XEvent]
  var committedEvents = IndexedSeq.empty[XEvent]
  var returnRightAwayTo: Option[ActorRef] = None

  startWith(FetchingPendingEvents, NoData)

  when(FetchingPendingEvents) {
    case Event(EventPersistence.EventsLoaded(events), _) =>
      unstashAll()
      goto(Ready)

    case Event(_, _) =>
      stash()
      stay()
  }

  val handleGetNextEvent: StateFunction = {
    case Event(ForgetEvent(event), _) =>
      pendingEventsPersistence ! EventPersistence.RemoveEvents(entityId, Seq(event.eventId))
      stay()

    case Event(GetNextEvent,_) if committedEvents.isEmpty =>
      returnRightAwayTo = Some(sender())
      stay()

    case Event(GetNextEvent, _) =>
      val next = committedEvents.head
      val rest = committedEvents.tail
      committedEvents = rest
      sender ! NextEvent(next)
      stay()
  }

  when(Ready)(handleGetNextEvent)
  when(Ready) {
    case Event(PutNextEvent(event), _) =>
      persistEvent(event)
  }

  def persistEvent(event: XEvent) = {
    pendingEventsPersistence ! EventPersistence.PersistEvent(event)
    goto(WaitingForWriteToComplete) using Write(sender(), event)
  }

  when(WaitingForWriteToComplete)(handleGetNextEvent)
  when(WaitingForWriteToComplete) {
    case Event(EventPersistence.EventPersisted(eventId), Write(sender, event)) if event.eventId == eventId =>
      sender ! EventRecorded(event.eventId.requestId)
      committedEvents = committedEvents :+ event
      if (transientEvents.isEmpty) {
        goto(Ready) using NoData
      }
      else {
        val next = transientEvents.head
        val rest = transientEvents.tail
        transientEvents = rest
        persistEvent(next)
      }

    case Event(PutNextEvent(event), _) =>
      transientEvents = transientEvents :+ event
      stay()
  }

  onTransition {
    case _ -> _ =>
      returnRightAwayTo match {
        case Some(replyTo) =>
          self.tell(GetNextEvent, replyTo)
          returnRightAwayTo = None
        case None =>
          // Do nothing.
      }
  }
}
