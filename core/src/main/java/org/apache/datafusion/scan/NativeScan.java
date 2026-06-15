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

/**
 * Raw native bindings to the {@code datafusion_scan_jni} shim.
 *
 * <p>Every method is a thin pass-through to the in-process scan core. Arrow data is never marshaled
 * across this boundary: the {@code *Addr} arguments are the memory addresses of {@code
 * org.apache.arrow.c.ArrowSchema} / {@code ArrowArrayStream} structs allocated by arrow-java, which
 * the native side fills in place. Callers should use {@link DatafusionScan} rather than these
 * directly.
 */
final class NativeScan {

  static {
    ScanNativeLoader.load();
  }

  private NativeScan() {}

  /** Probe a provider's output schema into the {@code ArrowSchema} at {@code schemaAddr}. */
  static native void providerSchema(String provider, byte[] config, long schemaAddr);

  /**
   * Plan a scan. Returns an opaque handle; release it with {@link #closeScan(long)}.
   *
   * @param provider registered builder name (e.g. {@code datafusion.listing})
   * @param config serialized {@code ScanConfig}
   * @param scanRequest serialized {@code ScanRequest} (pushdown), or empty for none
   */
  static native long createScan(String provider, byte[] config, byte[] scanRequest);

  /** Output partition count of a planned scan. */
  static native int partitionCount(long handle);

  /** Execute one partition into the {@code ArrowArrayStream} at {@code streamAddr}. */
  static native void executeStreamPartition(long handle, int partition, long streamAddr);

  /** Execute the whole plan as one coalesced stream into {@code streamAddr}. */
  static native void executeStream(long handle, long streamAddr);

  /** Drop a planned scan. Null-safe. */
  static native void closeScan(long handle);
}
