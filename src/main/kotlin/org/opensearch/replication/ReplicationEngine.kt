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

package org.opensearch.replication

import org.opensearch.index.engine.DeletionStrategy
import org.opensearch.index.engine.Engine
import org.opensearch.index.engine.EngineConfig
import org.opensearch.index.engine.IndexingStrategy
import org.opensearch.index.engine.InternalEngine
import org.opensearch.index.seqno.SequenceNumbers

class ReplicationEngine(config: EngineConfig) : InternalEngine(config) {

    /**
     * Replicated operations arrive with Origin.PRIMARY but must be planned using the
     * non-primary path to avoid assertions in the primary planning flow (e.g.
     * tryAcquireInFlightDocs) that don't apply to cross-cluster replicated ops.
     */
    override fun indexingStrategyForOperation(index: Engine.Index): IndexingStrategy {
        return planIndexingAsNonPrimary(index)
    }

    override fun deletionStrategyForOperation(delete: Engine.Delete): DeletionStrategy {
        return planDeletionAsNonPrimary(delete)
    }

    override fun assertPrimaryIncomingSequenceNumber(origin: Engine.Operation.Origin, seqNo: Long): Boolean {
        assert(origin == Engine.Operation.Origin.PRIMARY) { "Expected origin PRIMARY for replicated ops but was $origin" }
        assert(seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) { "Expected valid sequence number for replicated op but was unassigned" }
        return true
    }

    override fun generateSeqNoForOperationOnPrimary(operation: Engine.Operation): Long {
        check(operation.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) { "Expected valid sequence number for replicate op but was unassigned"}
        return operation.seqNo()
    }

    override fun assertNonPrimaryOrigin(operation: Engine.Operation): Boolean {
        return true
    }
}
