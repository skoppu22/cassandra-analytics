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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.JavaConverters;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Port of {@code dockertests/tests/sbr/test_udts.py}: exercises the bulk reader on two UDTs
 * including a nested map-of-maps — {@code frozen<map<int, frozen<map<bigint, uuid>>>>}.
 *
 * <p>UDT field access uses {@link Row#getStruct(int)} positional accessors since the schema maps
 * each UDT to a nested struct in Spark.
 */
class UdtsReadTest extends DockertestBase
{
    static final int NUM_SSTABLES = 2;
    static final int NUM_ROWS = 50;
    static final int OUTER_MAP_ENTRIES = 3;
    static final int INNER_MAP_ENTRIES = 5;
    static final int SET_ENTRIES = 5;

    QualifiedName table = uniqueTestTableFullName(TEST_KEYSPACE, "udts");
    Map<Long, Udt1> expectedUdt1 = new HashMap<>();
    Map<Long, Udt2> expectedUdt2 = new HashMap<>();

    @Test
    void testUdtsRead()
    {
        Dataset<Row> data = bulkReaderDataFrame(table).load();
        assertThat(data.count()).isEqualTo(expectedUdt1.size());

        for (Row row : data.collectAsList())
        {
            long key = row.getLong(0);
            Udt1 u1 = expectedUdt1.get(key);
            Udt2 u2 = expectedUdt2.get(key);
            assertThat(u1).isNotNull();

            Row udt1Row = row.getStruct(1);
            assertThat(udt1Row.getLong(0)).isEqualTo(u1.a);
            assertThat(udt1Row.getString(1)).isEqualTo(u1.b);

            Map<Object, Object> outerMap = JavaConverters.mapAsJavaMapConverter(udt1Row.getMap(2)).asJava();
            assertThat(outerMap).hasSize(u1.nested.size());
            for (Map.Entry<Object, Object> outerEntry : outerMap.entrySet())
            {
                int outerKey = ((Number) outerEntry.getKey()).intValue();
                assertThat(u1.nested).containsKey(outerKey);
                Map<Long, String> expectedInner = u1.nested.get(outerKey);

                @SuppressWarnings("unchecked")
                Map<Object, Object> actualInner = JavaConverters.mapAsJavaMapConverter(
                    (scala.collection.Map<Object, Object>) outerEntry.getValue()).asJava();
                assertThat(actualInner).hasSize(expectedInner.size());
                for (Map.Entry<Object, Object> innerEntry : actualInner.entrySet())
                {
                    long innerKey = ((Number) innerEntry.getKey()).longValue();
                    assertThat(expectedInner).containsKey(innerKey);
                    assertThat(innerEntry.getValue().toString()).isEqualTo(expectedInner.get(innerKey));
                }
            }

            Row udt2Row = row.getStruct(2);
            assertThat(udt2Row.getInt(0)).isEqualTo(u2.a);
            List<Object> actualSet = udt2Row.getList(1);
            Set<String> actualSetStrings = actualSet.stream().map(Object::toString).collect(Collectors.toSet());
            assertThat(actualSetStrings).isEqualTo(u2.b);
        }
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        cluster.schemaChange(String.format(
            "CREATE TYPE IF NOT EXISTS %s.test_udt1 (a bigint, b text, c frozen<map<int, frozen<map<bigint, uuid>>>>);",
            TEST_KEYSPACE));
        cluster.schemaChange(String.format(
            "CREATE TYPE IF NOT EXISTS %s.test_udt2 (a int, b frozen<set<text>>);",
            TEST_KEYSPACE));
        createTestTable(table, "CREATE TABLE IF NOT EXISTS %s (a bigint, b frozen<test_udt1>, c frozen<test_udt2>, "
                               + "PRIMARY KEY (a));");
        disableAutoCompaction(table);

        Random random = new Random(0);
        for (int s = 0; s < NUM_SSTABLES; s++)
        {
            for (long partitionKey = 0; partitionKey < NUM_ROWS; partitionKey++)
            {
                long u1a = Math.abs(random.nextLong()) % 100_000_000L;
                String u1b = UUID.randomUUID().toString().replace("-", "");

                Map<Integer, Map<Long, String>> nested = new LinkedHashMap<>();
                StringBuilder outerCql = new StringBuilder("{");
                int outerIdx = 0;
                Set<Integer> usedOuterKeys = new HashSet<>();
                for (int o = 0; o < OUTER_MAP_ENTRIES; o++)
                {
                    int outerKey;
                    do
                    {
                        outerKey = random.nextInt(100_000);
                    } while (!usedOuterKeys.add(outerKey));

                    Map<Long, String> inner = new LinkedHashMap<>();
                    StringBuilder innerCql = new StringBuilder("{");
                    Set<Long> usedInnerKeys = new HashSet<>();
                    int innerIdx = 0;
                    for (int i = 0; i < INNER_MAP_ENTRIES; i++)
                    {
                        long innerKey;
                        do
                        {
                            innerKey = Math.abs(random.nextLong()) % 100_000_000L;
                        } while (!usedInnerKeys.add(innerKey));
                        UUID uuid = UUID.randomUUID();
                        inner.put(innerKey, uuid.toString());
                        if (innerIdx++ > 0) innerCql.append(",");
                        innerCql.append(innerKey).append(":").append(uuid);
                    }
                    innerCql.append("}");
                    nested.put(outerKey, inner);
                    if (outerIdx++ > 0) outerCql.append(",");
                    outerCql.append(outerKey).append(":").append(innerCql);
                }
                outerCql.append("}");

                int u2a = Math.abs(random.nextInt()) % 100_000;
                Set<String> u2b = new HashSet<>();
                StringBuilder setCql = new StringBuilder("{");
                int setIdx = 0;
                for (int i = 0; i < SET_ENTRIES; i++)
                {
                    String s2 = UUID.randomUUID().toString().replace("-", "");
                    u2b.add(s2);
                    if (setIdx++ > 0) setCql.append(",");
                    setCql.append("'").append(s2).append("'");
                }
                setCql.append("}");

                execute(String.format("INSERT INTO %s (a, b, c) VALUES (%d, {a:%d, b:'%s', c:%s}, {a:%d, b:%s});",
                                      table, partitionKey, u1a, u1b, outerCql, u2a, setCql));

                expectedUdt1.put(partitionKey, new Udt1(u1a, u1b, nested));
                expectedUdt2.put(partitionKey, new Udt2(u2a, u2b));
            }
            flushKeyspace(table);
        }
    }

    static class Udt1
    {
        final long a;
        final String b;
        final Map<Integer, Map<Long, String>> nested;

        Udt1(long a, String b, Map<Integer, Map<Long, String>> nested)
        {
            this.a = a;
            this.b = b;
            this.nested = nested;
        }
    }

    static class Udt2
    {
        final int a;
        final Set<String> b;

        Udt2(int a, Set<String> b)
        {
            this.a = a;
            this.b = b;
        }
    }
}
