/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.replication.task.cleanup

import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATED_INDEX_SETTING
import org.opensearch.replication.action.index.block.IndexBlockUpdateType
import org.opensearch.replication.action.index.block.UpdateIndexBlockAction
import org.opensearch.replication.action.index.block.UpdateIndexBlockRequest
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.store.ReplicationMetadata
import org.opensearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import org.opensearch.replication.util.suspendExecute
import org.opensearch.replication.util.waitForClusterStateUpdate
import kotlinx.coroutines.delay
import org.apache.logging.log4j.LogManager
import org.opensearch.transport.client.Client
import org.opensearch.cluster.AckedClusterStateUpdateTask
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.core.index.shard.ShardId
import org.opensearch.persistent.PersistentTasksCustomMetadata
import org.opensearch.persistent.PersistentTasksCustomMetadata.PersistentTask
import org.opensearch.persistent.PersistentTasksService
import org.opensearch.persistent.RemovePersistentTaskAction
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.OpenSearchException
import org.opensearch.transport.ConnectTransportException
import java.util.concurrent.TimeoutException
import java.util.function.Predicate

/**
 * Comprehensive task cleanup manager that provides robust cleanup of replication tasks
 * and associated resources with retry logic and detailed error handling.
 * 
 * Also handles cleanup of stale artifacts (blocks, settings, persistent tasks) that may
 * remain after incomplete stop operations or system failures.
 */
class TaskCleanupManager @Inject constructor(
    private val persistentTasksService: PersistentTasksService,
    private val clusterService: ClusterService,
    private val client: Client,
    private val replicationMetadataManager: ReplicationMetadataManager,
    private val staleArtifactDetector: StaleArtifactDetector
) {
    
    companion object {
        private val log = LogManager.getLogger(TaskCleanupManager::class.java)
        const val MAX_CLEANUP_RETRIES = 3
        const val CLEANUP_RETRY_DELAY_MS = 1000L
    }

    suspend fun cleanupAllReplicationTasks(indexName: String, replMetadata: ReplicationMetadata? = null): CleanupResult {
        val failures = mutableListOf<CleanupFailure>()
        
        return try {
            // Note: We do NOT explicitly cancel shard/index tasks here.
            // Tasks will cancel themselves when they detect cluster state changes via ClusterStateListener.
            // We only clean up unassigned persistent tasks and retention leases.
            
            val leaseResult = removeRetentionLeases(indexName, replMetadata)
            val persistentResult = removePersistentTasks(indexName)
            
            // Note: We do NOT explicitly clean up stats from FollowerClusterStats here.
            // Stats are cleaned up automatically by ShardReplicationTask.cleanup() when tasks are cancelled.
            
            failures.addAll(leaseResult.failures + persistentResult.failures)
            
            CleanupResult(failures.isEmpty(), 0, 0,
                leaseResult.leasesRemoved, persistentResult.tasksRemoved, failures)
        } catch (e: Exception) {
            log.error("Cleanup error for $indexName", e)
            CleanupResult(false, 0, 0, 0, 0, 
                listOf(CleanupFailure(CleanupFailure.COMPONENT_CLEANUP_MANAGER, null, e.message ?: "Unknown error", false)))
        }
    }

    /**
     * Suspends replication tasks without removing retention leases.
     * Used during PAUSE operation to keep retention leases intact for later RESUME.
     */
    suspend fun suspendReplicationTasks(indexName: String): CleanupResult {
        val failures = mutableListOf<CleanupFailure>()
        
        return try {
            // Note: We do NOT explicitly cancel shard/index tasks here.
            // Tasks will cancel themselves when they detect cluster state changes via ClusterStateListener.
            // We only clean up unassigned persistent tasks.
            
            val persistentResult = removePersistentTasks(indexName)
            
            // Note: We do NOT explicitly clean up stats from FollowerClusterStats here.
            // Stats are cleaned up automatically by ShardReplicationTask.cleanup() when tasks are cancelled.
            
            failures.addAll(persistentResult.failures)
            
            CleanupResult(failures.isEmpty(), 0, 0,
                0, persistentResult.tasksRemoved, failures)
        } catch (e: Exception) {
            log.error("Suspend tasks error for $indexName", e)
            CleanupResult(false, 0, 0, 0, 0, 
                listOf(CleanupFailure(CleanupFailure.COMPONENT_CLEANUP_MANAGER, null, e.message ?: "Unknown error", false)))
        }
    }
    
    suspend fun removeRetentionLeases(indexName: String, replMetadata: ReplicationMetadata? = null): RetentionLeaseCleanupResult {
        val failures = mutableListOf<CleanupFailure>()
        var leasesRemoved = 0
        
        // If no metadata provided, skip retention lease removal
        if (replMetadata == null) {
            log.debug("No replication metadata provided for $indexName, skipping retention lease removal")
            return RetentionLeaseCleanupResult(0, emptyList())
        }
        
        try {
            val retentionLeaseHelper = RemoteClusterRetentionLeaseHelper(
                clusterService.clusterName.value(), 
                clusterService.state().metadata.clusterUUID(), 
                client.getRemoteClusterClient(replMetadata.connectionName)
            )
            
            // Attempt to remove retention lease - this may fail if leader index has validation issues
            try {
                retentionLeaseHelper.attemptRemoveRetentionLease(clusterService, replMetadata, indexName)
                leasesRemoved = 1
            } catch (e: IllegalArgumentException) {
                // Leader index validation failed (e.g., synonym file issues) - log and continue
                // This can happen when getLeaderIndexMetadata() validates index settings
                log.warn("Failed to validate leader index during retention lease removal for $indexName: ${e.message}. " +
                        "Retention lease removal skipped but operation will continue.")
            } catch (e: OpenSearchException) {
                // Check if this is a wrapped IllegalArgumentException (e.g., from index validation)
                val cause = e.cause
                if (cause is IllegalArgumentException || e.message?.contains("Failed to verify index") == true) {
                    log.warn("Failed to validate leader index during retention lease removal for $indexName: ${e.message}. " +
                            "Retention lease removal skipped but operation will continue.")
                } else {
                    // Other OpenSearch errors - log but don't fail
                    log.warn("Failed to remove retention lease for $indexName: ${e.message}")
                }
            } catch (e: Throwable) {
                // Catch all other throwables (including errors) - log but don't fail
                log.warn("Failed to remove retention lease for $indexName: ${e.message}", e)
            }
        } catch (e: Throwable) {
            // Errors during helper initialization or remote client access - log but don't fail the operation
            log.warn("Failed to initialize retention lease helper for $indexName: ${e.message}", e)
        }
        
        return RetentionLeaseCleanupResult(leasesRemoved, failures)
    }

    suspend fun removePersistentTasks(indexName: String): PersistentTaskCleanupResult {
        val failures = mutableListOf<CleanupFailure>()
        var tasksRemoved = 0
        
        try {
            // Wait for tasks to become unassigned after cancellation
            // This gives the task framework time to mark cancelled tasks as unassigned
            var retryCount = 0
            val maxRetries = 10
            val retryDelayMs = 100L
            
            while (retryCount < maxRetries) {
                val allTasks = clusterService.state().metadata.custom<PersistentTasksCustomMetadata>(
                    PersistentTasksCustomMetadata.TYPE
                ) ?: return PersistentTaskCleanupResult(0, emptyList())
                
                val replicationTasks = allTasks.tasks()
                    .filter { staleArtifactDetector.isReplicationTaskForIndex(it, indexName) }
                
                val unassignedTasks = replicationTasks.filter { !it.isAssigned }
                val assignedTasks = replicationTasks.filter { it.isAssigned }
                
                // Remove all unassigned tasks
                unassignedTasks.forEach { task ->
                    try {
                        client.suspendExecute(RemovePersistentTaskAction.INSTANCE, RemovePersistentTaskAction.Request(task.id))
                        tasksRemoved++
                        log.debug("Removed unassigned persistent task: ${task.id} for index: $indexName")
                    } catch (e: Exception) {
                        failures.add(CleanupFailure(CleanupFailure.COMPONENT_PERSISTENT_TASK, task.id,
                            "Failed to remove from cluster state: ${e.message}", true))
                    }
                }
                
                // If there are still assigned tasks, wait and retry
                if (assignedTasks.isNotEmpty() && retryCount < maxRetries - 1) {
                    log.debug("Found ${assignedTasks.size} assigned tasks for $indexName, waiting for them to become unassigned (retry $retryCount/$maxRetries)")
                    delay(retryDelayMs * (retryCount + 1))
                    retryCount++
                } else {
                    // Either no assigned tasks left, or we've exhausted retries
                    if (assignedTasks.isNotEmpty()) {
                        log.warn("Still found ${assignedTasks.size} assigned tasks for $indexName after $maxRetries retries: ${assignedTasks.map { it.id }}")
                    }
                    break
                }
            }
        } catch (e: Exception) {
            failures.add(CleanupFailure(CleanupFailure.COMPONENT_PERSISTENT_TASK, null,
                "Failed to access cluster state: ${e.message}", false))
        }
        
        return PersistentTaskCleanupResult(tasksRemoved, failures)
    }
    
    suspend fun cleanupStaleArtifacts(indexName: String): StaleArtifactCleanupResult {
        val artifactReport = staleArtifactDetector.detectStaleArtifacts(indexName)
        if (!artifactReport.hasStaleArtifacts) return StaleArtifactCleanupResult(indexName, true, 0, 0, emptyList())
        
        val failures = mutableListOf<CleanupFailure>()
        var artifactsRemoved = 0
        
        artifactReport.artifacts.forEach { artifact ->
            try {
                val removed = when (artifact.type) {
                    StaleArtifactType.REPLICATION_BLOCK -> cleanupReplicationBlock(indexName)
                    StaleArtifactType.REPLICATED_INDEX_SETTING -> cleanupReplicatedIndexSetting(indexName)
                    StaleArtifactType.PERSISTENT_TASK -> artifact.taskId?.let { cleanupPersistentTask(it) } ?: false
                }
                if (removed) artifactsRemoved++
            } catch (e: Exception) {
                failures.add(CleanupFailure("stale-artifact-${artifact.type.name.lowercase()}", 
                    artifact.taskId, e.message ?: "Unknown error", true))
            }
        }
        
        return StaleArtifactCleanupResult(indexName, failures.isEmpty(), 
            artifactReport.artifacts.size, artifactsRemoved, failures)
    }
    
    suspend fun cleanupAllStaleArtifacts(): Map<String, StaleArtifactCleanupResult> {
        return staleArtifactDetector.detectAllStaleArtifacts().mapValues { (indexName, _) ->
            try {
                cleanupStaleArtifacts(indexName)
            } catch (e: Exception) {
                StaleArtifactCleanupResult(indexName, false, 0, 0, 
                    listOf(CleanupFailure("stale-artifact-cleanup", null, e.message ?: "Unknown error", true)))
            }
        }
    }
    
    private suspend fun cleanupReplicationBlock(indexName: String) = try {
        client.suspendExecute(UpdateIndexBlockAction.INSTANCE, 
            UpdateIndexBlockRequest(indexName, IndexBlockUpdateType.REMOVE_BLOCK), injectSecurityContext = true).isAcknowledged
    } catch (e: Exception) { false }
    
    private suspend fun cleanupReplicatedIndexSetting(indexName: String) = try {
        val response: AcknowledgedResponse = clusterService.waitForClusterStateUpdate("remove_replicated_setting_$indexName") { listener ->
            RemoveReplicatedSettingTask(indexName, listener)
        }
        response.isAcknowledged
    } catch (e: Exception) { false }
    
    private suspend fun cleanupPersistentTask(taskId: String) = try {
        client.suspendExecute(RemovePersistentTaskAction.INSTANCE, RemovePersistentTaskAction.Request(taskId))
        true
    } catch (e: Exception) { false }
    
    private inner class RemoveReplicatedSettingTask(private val indexName: String, listener: ActionListener<AcknowledgedResponse>) : 
        AckedClusterStateUpdateTask<AcknowledgedResponse>(
            org.opensearch.transport.client.Requests.closeIndexRequest(indexName), 
            listener
        ) {
        override fun execute(currentState: ClusterState): ClusterState {
            val currentIndexMetadata = currentState.metadata.index(indexName) ?: return currentState
            if (currentIndexMetadata.settings[REPLICATED_INDEX_SETTING.key] == null) return currentState
            
            val newIndexMetadata = IndexMetadata.builder(currentIndexMetadata)
                .settings(Settings.builder().put(currentIndexMetadata.settings).putNull(REPLICATED_INDEX_SETTING.key))
                .settingsVersion(1 + currentIndexMetadata.settingsVersion)
            
            return ClusterState.builder(currentState)
                .metadata(Metadata.builder(currentState.metadata).put(newIndexMetadata)).build()
        }
        
        override fun newResponse(acknowledged: Boolean) = AcknowledgedResponse(acknowledged)
    }
    
    private fun handleCleanupFailure(failure: CleanupFailure) = when {
        failure.error.contains("task not found") || failure.error.contains("RetentionLeaseNotFoundException") ||
        failure.error.contains("no such index") -> RecoveryAction.LOG_AND_CONTINUE
        failure.error.contains("timeout") || failure.error.contains("connection") || 
        failure.retryable -> RecoveryAction.RETRY_WITH_BACKOFF
        else -> RecoveryAction.LOG_AND_CONTINUE
    }
    
    private fun createCleanupFailure(component: String, taskId: String?, exception: Exception) =
        CleanupFailure(component, taskId, exception.message ?: exception.javaClass.simpleName, isRetryableException(exception))
    
    private fun isRetryableException(exception: Exception) = when (exception) {
        is TimeoutException, is ConnectTransportException -> true
        is OpenSearchException -> exception.status().status >= 500 || 
            exception.message?.contains("timeout") == true || exception.message?.contains("connection") == true
        else -> exception.message?.lowercase()?.let {
            it.contains("timeout") || it.contains("connection") || it.contains("network") || it.contains("unavailable")
        } ?: false
    }
}

// Data classes and enums

data class CleanupResult(
    val success: Boolean,
    val indexTasksRemoved: Int,
    val shardTasksRemoved: Int,
    val retentionLeasesRemoved: Int,
    val persistentTasksRemoved: Int,
    val failures: List<CleanupFailure>
)

data class RetentionLeaseCleanupResult(val leasesRemoved: Int, val failures: List<CleanupFailure>) {
    val success get() = failures.isEmpty()
}

data class PersistentTaskCleanupResult(val tasksRemoved: Int, val failures: List<CleanupFailure>) {
    val success get() = failures.isEmpty()
}

data class CleanupFailure(val component: String, val taskId: String?, val error: String, val retryable: Boolean) {
    companion object {
        const val COMPONENT_SHARD_TASK = "shard-task"
        const val COMPONENT_INDEX_TASK = "index-task"
        const val COMPONENT_RETENTION_LEASE = "retention-lease"
        const val COMPONENT_PERSISTENT_TASK = "persistent-task"
        const val COMPONENT_CLEANUP_MANAGER = "cleanup-manager"
    }
}

data class StaleArtifactCleanupResult(
    val indexName: String,
    val success: Boolean,
    val artifactsFound: Int,
    val artifactsRemoved: Int,
    val failures: List<CleanupFailure>
) {
    fun isCompleteSuccess() = success && artifactsFound == artifactsRemoved
}

enum class RecoveryAction { RETRY_WITH_BACKOFF, LOG_AND_CONTINUE, FAIL_OPERATION }
