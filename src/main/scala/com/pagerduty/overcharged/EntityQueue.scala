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

trait ExtendedLoggingFSM[State, Data] extends LoggingFSM[State, Data] {
  override def logDepth = 10

  onTermination {
    case StopEvent(FSM.Failure(_), state, data) => {
      val lastEvents = getLog.mkString("\n\t")
      // This is for low-level actor debugging, so we are not using Scheduler.Logging.
      log.error(s"Failure in $state with $data.\nEvent log:\n\t$lastEvents")
    }
  }
}


object EntityQueue {
  case object GetSnapshot
  case class SnapshotResult(snapshot: EntityData)

  case class RestoreFromSnapshot(snapshot: EntityData)
  case class RestoreFromEvent(event: XEvent)
  case class ProcessEvent(event: XEvent, sender: ActorRef)
  case object ReadyForNextEvent

  def props(
    entityFactory: (ActorContext, EntityId) => ActorRef,
    entityId: EntityId,
    entityPersistenceCtx: EntityPersistenceCtx
  ): Props = {
    Props(new EntityQueue(entityFactory, entityId, entityPersistenceCtx))
  }

  trait State
  case object WaitingForEvent extends State
  case object WaitingForSnapshot extends State
  case object WaitingForSnapshotPersistence extends State
  case object WaitingToFinishProcessing extends State


  trait Data
  case object NoData extends Data
  case class EventsSoFar(count: Int) extends Data
}

class EntityQueue(
  entityFactory: (ActorContext, EntityId) => ActorRef,
  entityId: EntityId,
  entityPersistenceCtx: EntityPersistenceCtx
) extends ExtendedLoggingFSM[EntityQueue.State, EntityQueue.Data] {
  import EntityQueue._
  import context.dispatcher

  val managed = entityFactory(context, entityId)
  val pendingQueue = context.actorOf(EventQueue.props(entityId, entityPersistenceCtx))

  val snapshots = entityPersistenceCtx.snapshotPersistence
  val completedEvents = entityPersistenceCtx.completedEventPersistence

  var processingEvent: Option[XEvent] = None

  pendingQueue ! EventQueue.GetNextEvent
  startWith(WaitingForEvent, EventsSoFar(0))

  val forwardIncomingThroughPending: StateFunction = {
    case Event(IncomingMessage(Destination(_, `entityId`), requestId, eventData), _) => {
      val eventId = XEventId(entityId, TimeUuid(), requestId)
      val event = XEvent(eventId, eventData)
      pendingQueue.forward(EventQueue.PutNextEvent(event))
      stay()
    }
  }

  when(WaitingForEvent) {
    case Event(EventQueue.NextEvent(event), EventsSoFar(count)) =>
      processingEvent = Some(event)
      managed ! ProcessEvent(event, sender())
      goto(WaitingToFinishProcessing) using EventsSoFar(count + 1)
  }

  when(WaitingToFinishProcessing) {
    case Event(ReadyForNextEvent, EventsSoFar(count)) =>
      processingEvent.foreach { event =>
        implicit val timeout: akka.util.Timeout = 10000.seconds
        completedEvents.ask(EventPersistence.PersistEvent(event)).map { _ =>
          pendingQueue ! EventQueue.ForgetEvent(event)
        }
      }
      processingEvent = None

      if (count > GlobalSettings.SnapshotInterval) {
        managed ! GetSnapshot
        goto(WaitingForSnapshot) using EventsSoFar(0)
      }
      else {
        pendingQueue ! EventQueue.GetNextEvent
        goto(WaitingForEvent)
      }
  }

  when(WaitingForSnapshot) {
    case Event(SnapshotResult(snapshot), _) =>
      snapshots ! SnapshotPersistence.PersistSnapshot(entityId, snapshot)
      goto(WaitingForSnapshotPersistence)
  }

  when(WaitingForSnapshotPersistence) {
    case Event(SnapshotPersistence.SnapshotPersisted(`entityId`), _) =>
      pendingQueue ! EventQueue.GetNextEvent
      goto(WaitingForEvent)
  }

  when(WaitingForEvent)(forwardIncomingThroughPending)
  when(WaitingForSnapshot)(forwardIncomingThroughPending)
  when(WaitingForSnapshotPersistence)(forwardIncomingThroughPending)
  when(WaitingToFinishProcessing)(forwardIncomingThroughPending)
}
