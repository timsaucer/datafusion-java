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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test of the {@code datafusion} Spark data source against a local SparkSession: the
 * connector reads a CSV through the DataFusion listing provider and the plain-C scan ABI, all the
 * way back to Spark rows. Covers schema inference, full scan, projection, and filter pushdown.
 */
class DatafusionSourceTest {

  private static SparkSession spark;

  @TempDir static Path tmp;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("datafusion-source-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  private Dataset<Row> read() throws Exception {
    Path csv = tmp.resolve("data.csv");
    Files.writeString(csv, "id,name\n1,a\n2,b\n3,c\n");
    return spark
        .read()
        .format("datafusion")
        .option("path", csv.toString())
        .option("format", "csv")
        .load();
  }

  @Test
  void inferredSchema() throws Exception {
    List<String> columns = Arrays.asList(read().schema().fieldNames());
    assertEquals(List.of("id", "name"), columns);
  }

  @Test
  void fullScanReturnsAllRows() throws Exception {
    assertEquals(3, read().count());
  }

  @Test
  void projectionSelectsColumns() throws Exception {
    Dataset<Row> names = read().select("name");
    assertEquals(List.of("name"), Arrays.asList(names.schema().fieldNames()));
    assertEquals(3, names.count());
  }

  @Test
  void filterPushdownReducesRows() throws Exception {
    Dataset<Row> filtered = read().filter(functions.col("id").geq(2));
    assertEquals(2, filtered.count());

    List<Long> ids = filtered.select("id").as(org.apache.spark.sql.Encoders.LONG()).collectAsList();
    assertTrue(ids.stream().allMatch(id -> id >= 2), "all surviving ids should be >= 2");
    assertEquals(2L + 3L, ids.stream().mapToLong(Long::longValue).sum());
  }
}
