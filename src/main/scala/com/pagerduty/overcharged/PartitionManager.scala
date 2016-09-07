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
import com.pagerduty.eris.config.{ConnectionPoolConfigBuilder, AstyanaxConfigBuilder}
import com.pagerduty.eris.custom.{PdClusterCtx, ErisPdSettings}
import com.pagerduty.eris.schema.SchemaLoader
import scala.concurrent.Future
import scala.util.control.NonFatal

object PartitionManager {
  case class NoSuchEntityTypeError(entityType: EntityType)

  def props(
    partitionPersistenceCtx: PartitionPersistenceCtx,
    entityTypeFactories: Map[EntityType, (ActorContext, EntityId) => ActorRef]
  ): Props = {
    Props(new PartitionManager(partitionPersistenceCtx, entityTypeFactories))
  }
}

class PartitionManager(
  val partitionPersistenceCtx: PartitionPersistenceCtx,
  val entityTypeFactories: Map[EntityType, (ActorContext, EntityId) => ActorRef]
)
    extends Actor with ActorLogging {
  import PartitionManager._

  def partitionId = partitionPersistenceCtx.partitionId

  val entityPersistenceCtx: EntityPersistenceCtx = EntityPersistenceCtx(
    partitionId,
    snapshotPersistence = context.actorOf(SnapshotPersistence.props(partitionPersistenceCtx)),
    pendingEventPersistence = context.actorOf(EventPersistence.props("pending", partitionPersistenceCtx)),
    completedEventPersistence = context.actorOf(EventPersistence.props("completed", partitionPersistenceCtx))
  )

  val typeManagers: Map[EntityType, ActorRef] = entityTypeFactories.map(startTypeManager)

  def receive = {
    case message @ IncomingMessage(Destination(entityType, _), _, _) =>
      typeManagers.get(entityType) match {
        case Some(typeManager) => typeManager.forward(message)
        case None => sender ! NoSuchEntityTypeError(entityType)
      }
  }

  def startTypeManager(typeFactory: (EntityType, (ActorContext, EntityId) => ActorRef)) = {
    val (entityType, entityFactory) = typeFactory
    val typeManager = context.actorOf(TypeManager.props(entityType, entityFactory, entityPersistenceCtx))
    entityType -> typeManager
  }
}
