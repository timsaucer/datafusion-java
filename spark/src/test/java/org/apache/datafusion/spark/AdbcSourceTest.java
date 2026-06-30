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

package org.apache.datafusion.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.adbc.core.TypedKey;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end test of the {@code adbc-datafusion} source against the example driver cdylib (built
 * from {@code examples/adbc-datafusion-driver}). It drives the full stack: Spark DataSourceV2 ->
 * arrow-adbc JNI driver manager -> the DataFusion ADBC cdylib -> our custom provider.
 *
 * <p>Requires the cdylib path in the {@code adbc.example.driver.path} system property; the test is
 * skipped (not failed) when it is absent, so the build is green without the native artifact. To
 * run:
 *
 * <pre>
 *   (cd examples/adbc-datafusion-driver &amp;&amp; cargo build --release)
 *   mvn -pl spark -am test -Dtest=AdbcSourceTest \
 *     -Dadbc.example.driver.path=$PWD/rust-target/release/libadbc_datafusion_example_driver.dylib
 * </pre>
 */
class AdbcSourceTest {

  private static final String ENTRYPOINT = "AdbcDatafusionExampleInit";
  private static final String TABLE = "example";

  private static SparkSession spark;
  private static String driverPath;

  @BeforeAll
  static void setUp() {
    driverPath = System.getProperty("adbc.example.driver.path");
    assumeTrue(
        driverPath != null && Files.exists(Path.of(driverPath)),
        "set -Dadbc.example.driver.path to the example driver cdylib to run this test");
    // local[8]: several task slots in one executor JVM, so the per-executor connection cache
    // (AdbcConnectionPool) is exercised across concurrent tasks.
    spark = SparkSession.builder().appName("adbc-source-test").master("local[8]").getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private Dataset<Row> load() {
    return spark
        .read()
        .format("adbc-datafusion")
        .option("driver", driverPath)
        .option("entrypoint", ENTRYPOINT)
        .option("table", TABLE)
        .load();
  }

  @Test
  void fullScanAcrossPartitions() {
    Dataset<Row> df = load();

    // Schema inferred via ADBC getTableSchema.
    assertEquals("id", df.schema().fields()[0].name());
    assertEquals("name", df.schema().fields()[1].name());

    // The provider has three partitions; with arrow-adbc >= 0.24 (executePartitioned
    // forwarded by the JNI bridge) they surface as multiple Spark input partitions.
    assertTrue(
        df.rdd().getNumPartitions() >= 2,
        "expected multiple Spark partitions, got " + df.rdd().getNumPartitions());

    List<Long> ids =
        df.collectAsList().stream().map(r -> r.getLong(0)).sorted().collect(Collectors.toList());
    assertEquals(List.of(1L, 2L, 3L), ids);
  }

  @Test
  void projectionPushdown() {
    List<String> columns =
        load().select("name").collectAsList().stream()
            .map(r -> r.getString(0))
            .sorted()
            .collect(Collectors.toList());
    assertEquals(List.of("alice", "bob", "carol"), columns);
  }

  @Test
  void filterPushdown() {
    List<Long> ids =
        load().filter("id > 1").collectAsList().stream()
            .map(r -> r.getLong(0))
            .sorted()
            .collect(Collectors.toList());
    assertEquals(List.of(2L, 3L), ids);
  }

  /**
   * Regression for the per-executor cache: across a multi-partition scan over many task slots, the
   * pool builds the native database (and so runs the driver's {@code ContextInit} provider
   * registration) exactly once per executor JVM -- not once per task. Each task still opens its own
   * connection off that one cached database.
   */
  @Test
  void providerRegisteredOncePerExecutor() {
    // Force reader tasks to run across the executor's slots.
    long rows = load().collectAsList().size();
    assertTrue(rows > 0, "scan returned rows");

    // All tasks share one driver+options key, so the pool builds exactly one native database for
    // this executor JVM regardless of partition/task count.
    assertEquals(
        1,
        AdbcConnectionPool.databasesBuiltForTesting(),
        "expected one native database per executor JVM, got "
            + AdbcConnectionPool.databasesBuiltForTesting());

    // One cached database, but each task opens its own connection off it.
    assertTrue(
        AdbcConnectionPool.taskConnectionsOpenedForTesting() > 0,
        "expected a per-task connection off the one cached database");
  }

  /**
   * Proves the driver's database-scoped plan cache is actually used. Every partition descriptor of
   * one query carries the same serialized physical plan; the driver deserializes it once and caches
   * it across all connections opened from the (per-executor) cached database. So the {@code N}
   * per-task connections of one scan must trigger exactly <b>one</b> deserialize, not {@code N}.
   *
   * <p>The driver exposes its deserialize count as the read-only int option {@code
   * adbc.datafusion.plan_deserialize_count}. The count is JVM-lifetime and shared across tests, so
   * we assert on the delta around a single scan, and use a distinct predicate so the plan bytes are
   * fresh (not already cached by another test) -- making the expected delta exactly 1.
   */
  @Test
  void planDeserializedOncePerExecutorNotPerTask() throws Exception {
    TypedKey<Long> deserializeCount =
        new TypedKey<>("adbc.datafusion.plan_deserialize_count", Long.class);

    // Distinct predicate -> distinct physical plan -> plan bytes not cached by another test.
    Dataset<Row> df = load().filter("id >= 1");
    int partitions = df.rdd().getNumPartitions();
    assertTrue(partitions >= 2, "need multiple partitions to test the collapse, got " + partitions);

    // Same options the scan uses -> same pool cache key -> same cached database (and counter).
    LinkedHashMap<String, String> raw = new LinkedHashMap<>();
    raw.put("driver", driverPath);
    raw.put("entrypoint", ENTRYPOINT);
    raw.put("table", TABLE);
    AdbcOptions opts = AdbcOptions.fromOptions(new CaseInsensitiveStringMap(raw));

    long before;
    try (AdbcConnectionPool.Lease lease = AdbcConnectionPool.acquire(opts)) {
      before = lease.connection().getOption(deserializeCount);
    }

    df.collectAsList(); // runs read_partition on `partitions` separate task connections

    long after;
    try (AdbcConnectionPool.Lease lease = AdbcConnectionPool.acquire(opts)) {
      after = lease.connection().getOption(deserializeCount);
    }

    assertEquals(
        1L,
        after - before,
        "the "
            + partitions
            + " partitions of one query must share ONE plan deserialize (database-scoped cache); "
            + "a per-connection cache would deserialize once per task = "
            + partitions);
  }
}
