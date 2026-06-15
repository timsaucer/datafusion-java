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

import org.apache.spark.sql.connector.read.InputPartition;

/**
 * A serializable slice of a scan shipped to an executor. Carries only bytes and an index -- never a
 * native handle, which would be meaningless in another process. The executor rebuilds the provider
 * from {@code config} and runs partition {@code index}.
 */
final class DatafusionInputPartition implements InputPartition {

  private static final long serialVersionUID = 1L;

  final String provider;
  final byte[] config;
  final byte[] scanRequest;
  final int index;

  DatafusionInputPartition(String provider, byte[] config, byte[] scanRequest, int index) {
    this.provider = provider;
    this.config = config;
    this.scanRequest = scanRequest;
    this.index = index;
  }
}
