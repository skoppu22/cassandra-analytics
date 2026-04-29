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

import java.util.Random;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Port of {@code dockertests/tests/sbr/test_max_index_interval.py}: creates a table with custom
 * {@code max_index_interval=4096 AND min_index_interval=32}, loads a larger dataset across
 * multiple SSTables, and performs a bulk read.
 *
 * <p>The original Python test scanned Spark stdout for the string
 * "Cannot read index summary because min_index_interval changed from" and failed if present.
 * In-process we cannot (cleanly) scan the bulk reader's log output; instead this test asserts the
 * bulk read completes and returns the expected row count — i.e. the reader does not bail out on
 * tables with custom index intervals.
 */
class MaxIndexIntervalReadTest extends DockertestBase
{
    static final int NUM_SSTABLES = 10;
    static final int NUM_ROWS = 100;
    static final int NUM_COLS = 8;

    QualifiedName table = uniqueTestTableFullName(TEST_KEYSPACE, "max_idx_interval");
    long expectedRowCount;

    @Test
    void testBulkReadSucceedsWithCustomIndexIntervals()
    {
        Dataset<Row> data = bulkReaderDataFrame(table).load();
        assertThat(data.count()).isEqualTo(expectedRowCount);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(table, "CREATE TABLE IF NOT EXISTS %s (a bigint, b bigint, c bigint, PRIMARY KEY (a, b)) "
                               + "WITH max_index_interval=4096 AND min_index_interval=32;");

        Random random = new Random(0);
        long partitionKey = 0;
        for (int s = 0; s < NUM_SSTABLES; s++)
        {
            for (int r = 0; r < NUM_ROWS; r++)
            {
                for (long clusteringKey = 0; clusteringKey < NUM_COLS; clusteringKey++)
                {
                    long value = random.nextInt(101);
                    execute(String.format("INSERT INTO %s (a, b, c) VALUES (%d, %d, %d);",
                                          table, partitionKey, clusteringKey, value));
                }
                partitionKey++;
            }
            flushKeyspace(table);
        }
        expectedRowCount = (long) NUM_SSTABLES * NUM_ROWS * NUM_COLS;
    }
}
