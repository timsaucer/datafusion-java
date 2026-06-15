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

import org.apache.datafusion.scan.DatafusionScan;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

/**
 * A planned DataFusion scan as a Spark {@link Scan}/{@link Batch}.
 *
 * <p>{@link #planInputPartitions()} runs on the driver: it plans once to learn the partition count,
 * then emits one serializable {@link DatafusionInputPartition} per partition carrying the config +
 * request bytes (never a native handle). Each executor rebuilds and runs its own partition.
 */
final class DatafusionScanImpl implements Scan, Batch {

  private final String provider;
  private final byte[] config;
  private final byte[] scanRequest;
  private final StructType readSchema;

  DatafusionScanImpl(String provider, byte[] config, byte[] scanRequest, StructType readSchema) {
    this.provider = provider;
    this.config = config;
    this.scanRequest = scanRequest;
    this.readSchema = readSchema;
  }

  /** The encoded ScanRequest bytes. Package-private for pushdown unit tests. */
  byte[] scanRequestBytes() {
    return scanRequest;
  }

  @Override
  public StructType readSchema() {
    return readSchema;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    int partitions;
    try (DatafusionScan scan = DatafusionScan.create(provider, config, scanRequest)) {
      partitions = scan.partitionCount();
    }
    InputPartition[] result = new InputPartition[partitions];
    for (int i = 0; i < partitions; i++) {
      result[i] = new DatafusionInputPartition(provider, config, scanRequest, i);
    }
    return result;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new DatafusionPartitionReaderFactory();
  }
}
