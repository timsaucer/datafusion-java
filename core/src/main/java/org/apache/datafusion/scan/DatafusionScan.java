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

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A planned scan over a DataFusion {@code TableProvider}, driven through the plain-C scan ABI.
 *
 * <p>This is the JVM-facing wrapper over {@link NativeScan}. Each scanned partition is returned as
 * an {@link ArrowReader} imported from a native {@code FFI_ArrowArrayStream} through the Arrow C
 * Stream interface, so record batches never pass through JNI -- they cross via the Arrow C Data
 * interface that arrow-java already speaks. This mirrors {@code DataFrame#collect}.
 *
 * <p>The provider and its parameters are supplied as a serialized {@code ScanConfig}; pushed-down
 * projection/filters/tuning as a serialized {@code ScanRequest}. Both are built with the generated
 * protobuf classes in {@code org.apache.datafusion.protobuf}.
 *
 * <p>Not thread-safe with respect to {@link #close()}: callers must not close a scan while a
 * partition execute is in flight on another thread.
 */
public final class DatafusionScan implements AutoCloseable {

  private final long handle;
  private boolean closed;

  private DatafusionScan(long handle) {
    this.handle = handle;
  }

  /**
   * Probe a provider's output schema without planning a scan.
   *
   * @param allocator allocator for the transient C schema struct
   * @param provider registered builder name (e.g. {@code datafusion.listing})
   * @param config serialized {@code ScanConfig}
   */
  public static Schema schema(BufferAllocator allocator, String provider, byte[] config) {
    ArrowSchema cSchema = ArrowSchema.allocateNew(allocator);
    CDataDictionaryProvider dictionaries = new CDataDictionaryProvider();
    NativeScan.providerSchema(provider, config, cSchema.memoryAddress());
    // importField takes ownership of the C struct and returns the struct-typed
    // root; its children are the table's columns.
    Field root = Data.importField(allocator, cSchema, dictionaries);
    return new Schema(root.getChildren());
  }

  /**
   * Plan a scan over {@code provider}.
   *
   * @param provider registered builder name
   * @param config serialized {@code ScanConfig}
   * @param scanRequest serialized {@code ScanRequest}, or {@code null}/empty for no pushdown
   */
  public static DatafusionScan create(String provider, byte[] config, byte[] scanRequest) {
    byte[] request = scanRequest == null ? new byte[0] : scanRequest;
    return new DatafusionScan(NativeScan.createScan(provider, config, request));
  }

  /** Number of output partitions this scan produces. */
  public int partitionCount() {
    return NativeScan.partitionCount(handle);
  }

  /**
   * Execute one partition. The returned {@link ArrowReader} owns the underlying stream; close it
   * when done. Safe to call concurrently for distinct partitions.
   */
  public ArrowReader executePartition(BufferAllocator allocator, int partition) {
    ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
    NativeScan.executeStreamPartition(handle, partition, stream.memoryAddress());
    return Data.importArrayStream(allocator, stream);
  }

  /** Execute the whole plan as a single coalesced reader. */
  public ArrowReader execute(BufferAllocator allocator) {
    ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
    NativeScan.executeStream(handle, stream.memoryAddress());
    return Data.importArrayStream(allocator, stream);
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    NativeScan.closeScan(handle);
  }
}
