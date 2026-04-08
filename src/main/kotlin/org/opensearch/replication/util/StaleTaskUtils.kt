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

package org.opensearch.replication.util

import org.apache.logging.log4j.LogManager
import org.opensearch.ResourceNotFoundException
import org.opensearch.cluster.service.ClusterService
import org.opensearch.persistent.PersistentTasksCustomMetadata
import org.opensearch.persistent.RemovePersistentTaskAction
import org.opensearch.transport.client.Client

/**
 * Utility object for inline stale replication task detection and cleanup.
 *
 * Replication tasks follow these ID patterns:
 * - Index task: "replication:index:{indexName}"
 * - Shard task: "replication:[{indexName}][{shardId}]"
 *
 * All operations are idempotent — calling them multiple times with the same
 * inputs produces the same result.
 */
object StaleTaskUtils {

    private val log = LogManager.getLogger(StaleTaskUtils::class.java)

    /**
     * Shard task pattern: replication:[indexName][shardId]
     * Captures the index name from the first bracket group.
     */
    private val SHARD_TASK_PATTERN = Regex("^replication:\\[([^\\]]+)\\]\\[\\d+\\]$")

    /**
     * Validates that no replication tasks (assigned or unassigned) remain for the given index.
     * Should be called after removeStaleTasksForIndex to ensure cleanup was complete.
     * @throws IllegalStateException if any replication tasks still exist
     */
    fun validateNoTasksRemaining(
        clusterService: ClusterService,
        indexName: String
    ) {
        val allTasks = clusterService.state().metadata
            .custom<PersistentTasksCustomMetadata>(PersistentTasksCustomMetadata.TYPE)
            ?: return

        val remainingTasks = allTasks.tasks().filter { task ->
            isReplicationTaskForIndex(task, indexName)
        }
        if (remainingTasks.isNotEmpty()) {
            log.warn("Found ${remainingTasks.size} active task(s) for index $indexName: ${remainingTasks.map { it.id }}")
            throw IllegalStateException(
                "Replication tasks are still active for index $indexName. " +
                "Please run the Stop Replication API to clean up before retrying."
            )
        }
    }
    fun findStaleTasksForIndex(
        clusterService: ClusterService,
        indexName: String
    ): List<PersistentTasksCustomMetadata.PersistentTask<*>> {
        val allTasks = clusterService.state().metadata
            .custom<PersistentTasksCustomMetadata>(PersistentTasksCustomMetadata.TYPE)
            ?: return emptyList()

        return allTasks.tasks().filter { task ->
            isReplicationTaskForIndex(task, indexName)
        }
    }

    // Determines whether a persistent task is a replication task for the given index.
    fun isReplicationTaskForIndex(
        task: PersistentTasksCustomMetadata.PersistentTask<*>,
        indexName: String
    ): Boolean {
        val taskId = task.id
        if (!taskId.startsWith("replication:")) return false

        // Index task format: replication:index:{indexName}
        if (taskId == "replication:index:$indexName") return true

        // Shard task format: replication:[{indexName}][{shardId}]
        val match = SHARD_TASK_PATTERN.find(taskId)
        return match?.groupValues?.get(1) == indexName
    }

    /**
     * Removes all stale replication tasks for the given index from the cluster state.
     * This operation is idempotent — if tasks have already been removed, the operation
     * succeeds silently. Each task removal is logged individually.
     * @return the number of tasks successfully removed
     * @throws IllegalStateException if a task removal fails (non-idempotent failure)
     */
    suspend fun removeStaleTasksForIndex(
        clusterService: ClusterService,
        client: Client,
        indexName: String
    ): Int {
        log.info("Starting stale task cleanup for index $indexName")

        val staleTasks = findStaleTasksForIndex(clusterService, indexName)
        if (staleTasks.isEmpty()) {
            log.info("No stale tasks found for index $indexName")
            return 0
        }

        log.info("Found ${staleTasks.size} stale task(s) for index $indexName: ${staleTasks.map { it.id }}")

        var removed = 0
        for (task in staleTasks) {
            try {
                val removeRequest = RemovePersistentTaskAction.Request(task.id)
                client.suspendExecute(RemovePersistentTaskAction.INSTANCE, removeRequest)
                removed++
                log.info("Removed stale task ${task.id} for index $indexName")
            } catch (e: ResourceNotFoundException) {
                // Task already removed - idempotent success
                log.debug("Stale task ${task.id} already removed for index $indexName")
                removed++
            } catch (e: Exception) {
                // Check if the root cause is ResourceNotFoundException (task already gone)
                if (hasCause(e, ResourceNotFoundException::class.java)) {
                    log.debug("Stale task ${task.id} already removed for index $indexName")
                    removed++
                } else {
                    log.error("Failed to remove stale task ${task.id} for index $indexName: ${e.message}", e)
                    throw IllegalStateException(
                        "Failed to cleanup stale task ${task.id} for index $indexName. " +
                        "Please run the Stop Replication API to clean up before retrying.",
                        e
                    )
                }
            }
        }

        log.info("Successfully cleaned up $removed stale task(s) for index $indexName")
        return removed
    }

    // Checks if the given exception or any of its causes is an instance of the specified type.
    private fun hasCause(e: Throwable, type: Class<out Throwable>): Boolean {
        var current: Throwable? = e
        while (current != null) {
            if (type.isInstance(current)) return true
            current = current.cause
        }
        return false
    }
}
