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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.datafusion.scan.DatafusionScan;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Reads one scan partition as Spark {@link ColumnarBatch}es, zero-copy.
 *
 * <p>The Arrow vectors imported from the native stream are wrapped directly in Spark {@link
 * ArrowColumnVector}s -- no per-cell copy. This requires the executor JVM to have a single
 * arrow-java (the cluster's Spark Arrow); the connector compiles against that version and never
 * bundles its own, so our import and Spark's {@code ArrowColumnVector} share the same classes.
 *
 * <p>Lifecycle: the underlying Arrow vectors are owned by the {@link ArrowReader}. We do not close
 * the {@link ColumnarBatch} (which would close those vectors a second time); {@link #close()}
 * closes the reader -- freeing the vectors once -- and then the allocator.
 */
final class DatafusionColumnarPartitionReader implements PartitionReader<ColumnarBatch> {

  private final BufferAllocator allocator;
  private final DatafusionScan scan;
  private final ArrowReader reader;
  private final VectorSchemaRoot root;
  private final ColumnarBatch batch;

  DatafusionColumnarPartitionReader(DatafusionInputPartition partition) {
    this.allocator = new RootAllocator();
    try {
      this.scan =
          DatafusionScan.create(partition.provider, partition.config, partition.scanRequest);
      this.reader = scan.executePartition(allocator, partition.index);
      this.root = reader.getVectorSchemaRoot();
      this.batch = new ColumnarBatch(wrap(root));
    } catch (IOException e) {
      allocator.close();
      throw new RuntimeException("failed to open scan partition " + partition.index, e);
    } catch (RuntimeException e) {
      allocator.close();
      throw e;
    }
  }

  /** Wrap each Arrow vector of the (reused) root as a Spark column vector, once. */
  private static ColumnVector[] wrap(VectorSchemaRoot root) {
    ColumnVector[] columns = new ColumnVector[root.getFieldVectors().size()];
    int i = 0;
    for (FieldVector vector : root.getFieldVectors()) {
      columns[i++] = new ArrowColumnVector(vector);
    }
    return columns;
  }

  @Override
  public boolean next() throws IOException {
    // The root's vectors are reloaded in place each batch; skip empty batches.
    while (reader.loadNextBatch()) {
      int rows = root.getRowCount();
      if (rows > 0) {
        batch.setNumRows(rows);
        return true;
      }
    }
    return false;
  }

  @Override
  public ColumnarBatch get() {
    return batch;
  }

  @Override
  public void close() throws IOException {
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
