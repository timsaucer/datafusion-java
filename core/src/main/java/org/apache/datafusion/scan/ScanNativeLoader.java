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
 * Loads the {@code datafusion_scan_jni} shim library.
 *
 * <p>This is the JVM adapter over the plain-C scan ABI exported by {@code
 * datafusion-scan-ffi}. The library is loaded from {@code java.library.path} (set it with {@code
 * -Djava.library.path=...} or the platform library-path environment variable so it can find the
 * built {@code libdatafusion_scan_jni}). Classpath bundling, as the core {@code datafusion_jni}
 * library does, is left to release packaging.
 */
final class ScanNativeLoader {

  private static final String LIBRARY_NAME = "datafusion_scan_jni";

  private static volatile boolean loaded;

  private ScanNativeLoader() {}

  static synchronized void load() {
    if (loaded) {
      return;
    }
    System.loadLibrary(LIBRARY_NAME);
    loaded = true;
  }
}
