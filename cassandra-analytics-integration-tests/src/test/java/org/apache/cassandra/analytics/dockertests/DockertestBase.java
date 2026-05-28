/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.analytics.dockertests;

import org.apache.cassandra.analytics.SharedClusterSparkIntegrationTestBase;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.testing.QualifiedName;

/**
 * Shared helpers for the port of the Python dockertests under
 * {@code cassandra-analytics-apple/dockertests/tests/sbr/}. The original tests insert data in
 * per-SSTable batches with nodetool flush in between to stress the bulk reader's SSTable merge path;
 * this base replicates that using {@link IInstance#flush(String)}.
 */
abstract class DockertestBase extends SharedClusterSparkIntegrationTestBase
{
    protected void flushKeyspace(String keyspace)
    {
        cluster.stream().forEach(instance -> instance.flush(keyspace));
    }

    protected void flushKeyspace(QualifiedName table)
    {
        flushKeyspace(table.keyspace());
    }

    /**
     * Disable auto-compaction for {@code keyspace} on every node, mirroring the dockertests'
     * {@code nodetool disableautocompaction <keyspace>} call. Without this, background compaction
     * can merge the per-batch SSTables created by {@link #flushKeyspace(QualifiedName)} between the
     * last flush and the bulk reader's snapshot, silently degrading the test from a multi-SSTable
     * merge read to a single-SSTable read.
     */
    protected void disableAutoCompaction(String keyspace)
    {
        cluster.stream().forEach(instance ->
            instance.nodetoolResult("disableautocompaction", keyspace).asserts().success());
    }

    protected void disableAutoCompaction(QualifiedName table)
    {
        disableAutoCompaction(table.keyspace());
    }

    protected void execute(String query)
    {
        ICoordinator coordinator = cluster.getFirstRunningInstance().coordinator();
        coordinator.execute(query, ConsistencyLevel.ALL);
    }
}
