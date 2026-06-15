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

import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.datafusion.scan.DatafusionScan;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

/**
 * Reads one scan partition into Spark {@link InternalRow}s.
 *
 * <p>Runs on the executor: rebuilds the scan from the partition's bytes, executes its single
 * partition, and streams batches in through the Arrow C Stream interface. Each batch is walked row
 * by row ({@link ArrowToInternalRow}) so no Arrow data crosses with Spark's bundled Arrow.
 */
final class DatafusionPartitionReader implements PartitionReader<InternalRow> {

  private final BufferAllocator allocator;
  private final DatafusionScan scan;
  private final ArrowReader reader;
  private final VectorSchemaRoot root;

  private int currentRow = -1;
  private int batchRows;

  DatafusionPartitionReader(DatafusionInputPartition partition) {
    this.allocator = new RootAllocator();
    try {
      this.scan =
          DatafusionScan.create(partition.provider, partition.config, partition.scanRequest);
      this.reader = scan.executePartition(allocator, partition.index);
      this.root = reader.getVectorSchemaRoot();
    } catch (IOException e) {
      allocator.close();
      throw new RuntimeException("failed to open scan partition " + partition.index, e);
    } catch (RuntimeException e) {
      allocator.close();
      throw e;
    }
  }

  @Override
  public boolean next() throws IOException {
    currentRow++;
    while (currentRow >= batchRows) {
      if (!reader.loadNextBatch()) {
        return false;
      }
      batchRows = root.getRowCount();
      currentRow = 0;
    }
    return true;
  }

  @Override
  public InternalRow get() {
    return ArrowToInternalRow.convert(root, currentRow);
  }

  @Override
  public void close() throws IOException {
    // Close in reverse order of acquisition; the reader owns the imported stream.
    try {
      reader.close();
    } finally {
      try {
        scan.close();
      } finally {
        allocator.close();
      }
    }
  }
}
