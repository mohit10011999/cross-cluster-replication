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
     * Finds all persistent replication tasks associated with the given index name.
     *
     * Searches the cluster state for persistent tasks whose IDs match either:
     * - Index task format: "replication:index:{indexName}"
     * - Shard task format: "replication:[{indexName}][{shardId}]"
     *
     * @param clusterService the cluster service to read cluster state from
     * @param indexName the target index name to match against task IDs
     * @return list of persistent tasks matching the index name, empty if none found
     */
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

    /**
     * Determines whether a persistent task is a replication task for the given index.
     *
     * Matches against two task ID formats:
     * - Index task: "replication:index:{indexName}" (exact match)
     * - Shard task: "replication:[{indexName}][{shardId}]" (regex match)
     *
     * @param task the persistent task to check
     * @param indexName the index name to match against
     * @return true if the task is a replication task for the given index
     */
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
     *
     * This operation is idempotent — if tasks have already been removed, the operation
     * succeeds silently. Each task removal is logged individually.
     *
     * @param clusterService the cluster service to read cluster state from
     * @param client the client to execute remove task actions
     * @param indexName the target index name whose stale tasks should be removed
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
            } catch (e: Exception) {
                log.error("Failed to remove stale task ${task.id} for index $indexName: ${e.message}", e)
                throw IllegalStateException(
                    "Failed to cleanup stale task ${task.id} for index $indexName. " +
                    "Please run the Stop Replication API to clean up before retrying.",
                    e
                )
            }
        }

        log.info("Successfully cleaned up $removed stale task(s) for index $indexName")
        return removed
    }
}
