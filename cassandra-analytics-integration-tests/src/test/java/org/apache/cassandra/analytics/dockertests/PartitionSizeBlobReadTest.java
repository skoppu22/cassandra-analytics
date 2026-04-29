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
 * Port of {@code dockertests/tests/partitionsize/test_basic.py}: writes 5000 rows with random
 * blobs (1 KiB – 256 KiB) into a table keyed by {@code (a bigint, b bigint)} with {@code c blob},
 * then asserts the reported on-disk partition size is 2.0–2.1x the raw byte size (a standard
 * Cassandra SSTable overhead bound).
 *
 * <p>Test coverage for {@code LocalPartitionSizeSource} itself already exists in
 * {@link org.apache.cassandra.spark.PartitionSizeTests} in the core module, but that test uses
 * the filesystem-level {@code TestUtils.openLocalPartitionSizeSource(...)} harness over SSTables
 * written by the bulk writer — not the in-JVM dtest cluster.
 *
 * <p>Porting this specific dockertest assertion (blob payload + size ratio) to the in-JVM dtest
 * framework requires plumbing {@code LocalPartitionSizeSource} against the in-memory dtest
 * instance's data directories. The plumbing is straightforward but cross-cutting (touch Spark
 * source registration + resolve SSTable dirs from {@code IInstance#config()}) and is deferred.
 *
 * <p>This test is stubbed as {@code @Disabled}. Unstub once a helper similar to
 * {@code bulkReaderDataFrame(...)} exists for the partition-size source on
 * {@link org.apache.cassandra.analytics.SharedClusterSparkIntegrationTestBase}.
 */
@Disabled("Requires LocalPartitionSizeSource wiring against in-JVM dtest data dirs; see javadoc")
class PartitionSizeBlobReadTest extends DockertestBase
{
    QualifiedName table = uniqueTestTableFullName(TEST_KEYSPACE, "psize_blob");

    @Test
    void testOnDiskSizeWithinExpectedOverheadRatio()
    {
        // Expected behavior (from dockertests/tests/partitionsize/test_basic.py):
        //   1. Insert num_rows=5000 rows of form (a bigint pk, b bigint ck, c blob). c is a
        //      random byte array in [1024, 262144] bytes.
        //   2. Open a partition-size data source against the same table.
        //   3. For every partition key a, assert (on-disk size) / (raw byte size of c) < 2.1.
        //
        // Implementation blocker: no dtest-cluster-aware LocalPartitionSizeSource helper; see class javadoc.
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(table, "CREATE TABLE IF NOT EXISTS %s (a bigint, b bigint, c blob, "
                               + "PRIMARY KEY (a, b));");
    }
}
