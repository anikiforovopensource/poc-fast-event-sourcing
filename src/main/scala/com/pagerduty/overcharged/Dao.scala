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

import com.netflix.astyanax.{Keyspace, Cluster}
import com.pagerduty.eris._
import com.pagerduty.eris.config.{ConnectionPoolConfigBuilder, AstyanaxConfigBuilder}
import com.pagerduty.eris.custom._
import com.pagerduty.eris.schema.SchemaLoader
import com.pagerduty.eris.serializers._
import com.pagerduty.widerow.EntryColumn
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.control.NonFatal

trait SnapshotDao {
  def storeSnapshot(entityId: EntityId, snapshot: EntityData): Future[Unit]
  def loadSnapshot(entityId: EntityId): Future[Option[EntityData]]
}

trait EventBatchDao {
  def columnFamilyName: String
  def persistEvents(rowId: RowId, events: Iterable[XEvent]): Future[Unit]
  def loadEvents(rowIds: Seq[RowId], fromCursor: Option[XEventId], limit: Int): Future[Seq[XEvent]]
  def removeEvents(rowId: RowId, events: Iterable[XEventId]): Future[Unit]
}

case class PersistenceCtx(
  pendingEventsBatchDao: EventBatchDao,
  completedEventsBatchDao: EventBatchDao,
  snapshotDao: SnapshotDao
)

object PersistenceCtx {
  def makeTestContext(): PersistenceCtx = {
    val clusterCtx = new PdClusterCtx(
      clusterName = "CassCluster",
      astyanaxConfig = AstyanaxConfigBuilder.build(
        asyncThreadPoolName = "AstyanaxAsync",
        asyncThreadPoolSize = 50
      )
        .setCqlVersion("3.0.0")
        .setTargetCassandraVersion("1.2")
      ,
      connectionPoolConfig = ConnectionPoolConfigBuilder.build(
        connectionPoolName = "CassConnectionPool",
        cassPort = GlobalSettings.CassPort,
        hosts = s"${GlobalSettings.CassHost}:${GlobalSettings.CassPort}",
        maxConnectionsPerHost = 50
      )
    )
    val cluster = clusterCtx.cluster
    val keyspace = cluster.getKeyspace("OverchargedTest")
    val pendingDao = new EventBatchDaoImpl("Pending", cluster, keyspace, new ErisPdSettings())
    val completedDao = new EventBatchDaoImpl("Completed", cluster, keyspace, new ErisPdSettings())
    val snapshotDao = new SnapshotDaoImpl(cluster, keyspace, new ErisPdSettings())
    val cdefs = pendingDao.columnFamilyDefs ++ completedDao.columnFamilyDefs ++ snapshotDao.columnFamilyDefs
    val schemaLoader = new SchemaLoader(cluster, cdefs)
    try {
      schemaLoader.dropSchema()
    }
    catch {
      case NonFatal(_) => //ignore
    }
    try {
      schemaLoader.loadSchema()
    }
    catch {
      case NonFatal(_) => //ignore
    }
    PersistenceCtx(pendingDao, completedDao, snapshotDao)
  }
}

case class PartitionPersistenceCtx(
  partitionId: PartitionId,
  pendingEventsBatchDao: EventBatchDao,
  completedEventsBatchDao: EventBatchDao,
  snapshotDao: SnapshotDao
)


class SnapshotDaoImpl(
  protected val cluster: Cluster,
  protected val keyspace: Keyspace,
  protected val settings: ErisPdSettings
)
    extends SnapshotDao with PdDao {

  protected implicit val executor: ExecutionContextExecutor = ExecutionContext.Implicits.global

  val storage = new WideRowMap(
    columnFamily[EntityId, String, EntityData]("Snapshots"),
    pageSize = 10
  )

  def storeSnapshot(entityId: EntityId, snapshot: EntityData): Future[Unit] = {
    DaoSnapshotCounter.bump()
    val batchUpdater = storage(entityId)
    val column = EntryColumn("Snapshot", snapshot)
    batchUpdater.queueInsert(column)
    batchUpdater.executeAsync()
  }
  def loadSnapshot(entityId: EntityId): Future[Option[EntityData]] = {
    Future.successful(None)
  }
}

class EventBatchDaoImpl(
  val columnFamilyName: String,
  protected val cluster: Cluster,
  protected val keyspace: Keyspace,
  protected val settings: ErisPdSettings
)
    extends EventBatchDao with PdDao {

  protected implicit val executor: ExecutionContextExecutor = ExecutionContext.Implicits.global

  type RowKey = String
  type XXEventId = (EntityId, TimeUuid, TimeUuid)

  protected val eventMap = new WideRowMap(
    columnFamily[RowKey, XXEventId, EventData](columnFamilyName),
    pageSize = 1000
  )

  def persistEvents(rowKey: RowKey, events: Iterable[XEvent]): Future[Unit] = {
    DaoWriteCounter.bump()
    val batchUpdater = eventMap(rowKey)
    for (event <- events) {
      val xxEventId = (event.eventId.entityId, event.eventId.eventId, event.eventId.requestId)
      val column = EntryColumn(xxEventId, event.eventData)
      batchUpdater.queueInsert(column)
    }
    batchUpdater.executeAsync()
  }

  def loadEvents(rowIds: Seq[RowId], fromCursor: Option[XEventId], limit: Int): Future[Seq[XEvent]] = {
    Future.failed(new RuntimeException("Unsupported yet."))
  }
  def removeEvents(rowKey: RowId, events: Iterable[XEventId]): Future[Unit] = {
    DaoRemoveCounter.bump()
    val batchUpdater = eventMap(rowKey)
    for (eventId <- events) {
      val xxEventId = (eventId.entityId, eventId.eventId, eventId.requestId)
      batchUpdater.queueRemove(xxEventId)
    }
    batchUpdater.executeAsync()
  }
}
