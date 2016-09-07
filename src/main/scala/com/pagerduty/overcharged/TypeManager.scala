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

object TypeManager {
  def props(
    entityType: EntityType,
    entityFactory: (ActorContext, EntityId) => ActorRef,
    entityPersistenceCtx: EntityPersistenceCtx
  ): Props = {
    Props(new TypeManager(entityType, entityFactory, entityPersistenceCtx))
  }
}

class TypeManager(
  val entityType: EntityType,
  val entityFactory: (ActorContext, EntityId) => ActorRef,
  val entityPersistenceCtx: EntityPersistenceCtx
)
    extends Actor with ActorLogging {

  def partitionId = entityPersistenceCtx.partitionId
  var liveEntities = Map.empty[EntityId, ActorRef]

  def receive = {
    case message @ IncomingMessage(Destination(`entityType`, entityId), _, _) =>
      val entityQueue = liveEntities.get(entityId) match {
        case Some(existing) => existing
        case None => loadEntityQueue(entityId)
      }
      entityQueue.forward(message)
  }

  def loadEntityQueue(entityId: EntityId): ActorRef = {
    val entityQueue = context.actorOf(EntityQueue.props(
      entityFactory, entityId, entityPersistenceCtx
    ))
    liveEntities += (entityId -> entityQueue)
    entityQueue
  }
}
