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

package org.apache.datafusion.scan;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import datafusion_common.DatafusionCommon.Column;
import datafusion_common.DatafusionCommon.ScalarValue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.datafusion.protobuf.BinaryExprNode;
import org.apache.datafusion.protobuf.CsvReadOptionsProto;
import org.apache.datafusion.protobuf.ListingSource;
import org.apache.datafusion.protobuf.LogicalExprNode;
import org.apache.datafusion.protobuf.ScanConfig;
import org.apache.datafusion.protobuf.ScanRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end exercise of the JNI shim: drive the {@code datafusion.listing} provider over a CSV
 * entirely from Java, confirming the Arrow C Stream produced by arrow-rs imports cleanly through
 * arrow-java's {@code Data.importArrayStream}. This is the proof that the C Stream ABI matches
 * across the two Arrow implementations through this path.
 */
class DatafusionScanTest {

  private static final String PROVIDER = "datafusion.listing";

  @TempDir Path tmp;

  /** Build a ScanConfig for a CSV listing source, using the generated protobuf builders. */
  private byte[] csvConfig(String path) {
    return ScanConfig.newBuilder()
        .setProvider(PROVIDER)
        .setListing(
            ListingSource.newBuilder()
                .addPaths(path)
                .setCsv(
                    CsvReadOptionsProto.newBuilder()
                        .setHasHeader(true)
                        .setDelimiter(',')
                        .setQuote('"')
                        .setFileExtension(".csv")
                        .build())
                .build())
        .build()
        .toByteArray();
  }

  @Test
  void inferredSchemaMatchesCsvHeader() throws Exception {
    Path csv = tmp.resolve("data.csv");
    Files.writeString(csv, "id,name\n1,a\n2,b\n3,c\n");
    byte[] config = csvConfig(csv.toString());

    try (BufferAllocator allocator = new RootAllocator()) {
      Schema schema = DatafusionScan.schema(allocator, PROVIDER, config);
      List<String> names = schema.getFields().stream().map(Field::getName).collect(toList());
      assertEquals(List.of("id", "name"), names);
    }
  }

  @Test
  void scansCsvRowsThroughArrowCStream() throws Exception {
    Path csv = tmp.resolve("data.csv");
    Files.writeString(csv, "id,name\n1,a\n2,b\n3,c\n");
    byte[] config = csvConfig(csv.toString());

    try (BufferAllocator allocator = new RootAllocator();
        DatafusionScan scan = DatafusionScan.create(PROVIDER, config, null)) {
      assertTrue(scan.partitionCount() >= 1, "expected at least one partition");

      long total = 0;
      int rows = 0;
      try (ArrowReader reader = scan.execute(allocator)) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        while (reader.loadNextBatch()) {
          rows += root.getRowCount();
          BigIntVector ids = (BigIntVector) root.getVector("id");
          for (int i = 0; i < root.getRowCount(); i++) {
            total += ids.get(i);
          }
        }
      }
      assertEquals(3, rows);
      assertEquals(1 + 2 + 3, total);
    }
  }

  @Test
  void projectionPrunesColumns() throws Exception {
    byte[] config = csvConfig(writeCsv());
    // Pushed projection: keep only "name".
    byte[] request = ScanRequest.newBuilder().addProjection("name").build().toByteArray();

    try (BufferAllocator allocator = new RootAllocator();
        DatafusionScan scan = DatafusionScan.create(PROVIDER, config, request)) {
      int rows = 0;
      try (ArrowReader reader = scan.execute(allocator)) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        List<String> cols =
            root.getSchema().getFields().stream().map(Field::getName).collect(toList());
        assertEquals(List.of("name"), cols, "projection should drop the id column");
        while (reader.loadNextBatch()) {
          rows += root.getRowCount();
        }
      }
      assertEquals(3, rows);
    }
  }

  @Test
  void filterPushdownSelectsRows() throws Exception {
    byte[] config = csvConfig(writeCsv());
    // Pushed filter: id >= 2.
    byte[] request =
        ScanRequest.newBuilder().addFilters(ByteString.copyFrom(idAtLeast(2))).build().toByteArray();

    try (BufferAllocator allocator = new RootAllocator();
        DatafusionScan scan = DatafusionScan.create(PROVIDER, config, request)) {
      long total = 0;
      int rows = 0;
      try (ArrowReader reader = scan.execute(allocator)) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        while (reader.loadNextBatch()) {
          rows += root.getRowCount();
          BigIntVector ids = (BigIntVector) root.getVector("id");
          for (int i = 0; i < root.getRowCount(); i++) {
            total += ids.get(i);
          }
        }
      }
      assertEquals(2, rows, "only id 2 and 3 pass the filter");
      assertEquals(2 + 3, total);
    }
  }

  @Test
  void limitCapsRows() throws Exception {
    byte[] config = csvConfig(writeCsv());
    // Pushed limit of 2 over the 3-row CSV.
    byte[] request = ScanRequest.newBuilder().setLimit(2).build().toByteArray();

    try (BufferAllocator allocator = new RootAllocator();
        DatafusionScan scan = DatafusionScan.create(PROVIDER, config, request)) {
      int rows = 0;
      try (ArrowReader reader = scan.execute(allocator)) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        while (reader.loadNextBatch()) {
          rows += root.getRowCount();
        }
      }
      assertEquals(2, rows, "limit should cap the scan at 2 rows");
    }
  }

  /** Serialize the LogicalExprNode for {@code id >= value}, as the engine's filter pushdown would. */
  private static byte[] idAtLeast(long value) {
    LogicalExprNode column =
        LogicalExprNode.newBuilder().setColumn(Column.newBuilder().setName("id")).build();
    LogicalExprNode literal =
        LogicalExprNode.newBuilder()
            .setLiteral(ScalarValue.newBuilder().setInt64Value(value))
            .build();
    return LogicalExprNode.newBuilder()
        .setBinaryExpr(
            BinaryExprNode.newBuilder().addOperands(column).addOperands(literal).setOp("GtEq"))
        .build()
        .toByteArray();
  }

  private String writeCsv() throws Exception {
    Path csv = tmp.resolve("data.csv");
    Files.writeString(csv, "id,name\n1,a\n2,b\n3,c\n");
    return csv.toString();
  }
}
