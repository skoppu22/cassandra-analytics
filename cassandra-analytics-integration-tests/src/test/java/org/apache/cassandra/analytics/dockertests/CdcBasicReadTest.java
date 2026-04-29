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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.testing.QualifiedName;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;

/**
 * Port of {@code dockertests/tests/cdc/test_basic.py}: inserts rows into a {@code WITH cdc=true}
 * table with explicit per-row {@code USING TIMESTAMP} values, runs a Spark CDC read
 * concurrently, and verifies output is ordered by {@code last_modified_timestamp}.
 *
 * <p>The OSS integration-tests module does not currently have a Spark DataSource that reads
 * Cassandra CDC events end-to-end. The CDC event-consumer framework lives in modules
 * {@code cassandra-analytics-cdc}, {@code cassandra-analytics-cdc-sidecar}, and
 * {@code cassandra-analytics-cdc-codec}, but there is no OSS {@code bulkReaderDataFrame}-style
 * CDC source wired into the in-JVM dtest framework. Building that wiring is a non-trivial
 * cross-module change (sidecar CDC server startup + commit-log streaming into a Spark source)
 * outside the scope of this dockertest port.
 *
 * <p>This test is stubbed as {@code @Disabled} with the expected behavior documented. Unstub once
 * an OSS Spark CDC source / {@code cdcDataFrame} helper exists on
 * {@link org.apache.cassandra.analytics.SharedClusterSparkIntegrationTestBase}.
 */
@Disabled("Requires an OSS Spark CDC source in cassandra-analytics-integration-tests; see javadoc")
class CdcBasicReadTest extends DockertestBase
{
    QualifiedName table = uniqueTestTableFullName(TEST_KEYSPACE, "cdc_basic");

    @Test
    void testCdcRowsOrderedByTimestamp()
    {
        // Expected behavior (from dockertests/tests/cdc/test_basic.py):
        //   1. Insert num_rows=1000 rows into keyspace.table USING TIMESTAMP now + v*1000 (micros).
        //   2. Concurrently run a Spark CDC read job that consumes the commit log.
        //   3. Collect rows from the job output, sort by last_modified_timestamp ascending,
        //      and assert the ordered sequence equals the insertion order (a == b == c == v).
        //
        // Implementation blocker: no OSS bulkReaderDataFrame-style CDC source. See class javadoc.
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(table, "CREATE TABLE IF NOT EXISTS %s (a bigint, b bigint, c bigint, "
                               + "PRIMARY KEY (a, b)) WITH cdc=true;");
    }
}
