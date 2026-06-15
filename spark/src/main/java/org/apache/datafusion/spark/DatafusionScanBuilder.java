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

import java.util.List;

import org.apache.datafusion.protobuf.ScanRequest;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

import com.google.protobuf.ByteString;

/**
 * Captures Spark's projection, filter, and limit pushdown, encoding them into the {@code
 * ScanRequest} the scan ABI consumes.
 */
final class DatafusionScanBuilder
    implements ScanBuilder,
        SupportsPushDownRequiredColumns,
        SupportsPushDownFilters,
        SupportsPushDownLimit {

  private final String provider;
  private final byte[] config;

  private StructType requiredSchema;
  private Filter[] pushedFilters = new Filter[0];
  private List<byte[]> pushedFilterBytes = List.of();
  private int limit = -1;

  DatafusionScanBuilder(StructType fullSchema, String provider, byte[] config) {
    this.provider = provider;
    this.config = config;
    this.requiredSchema = fullSchema;
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.requiredSchema = requiredSchema;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    SparkFilters.Result result = SparkFilters.split(filters);
    this.pushedFilters = result.pushedFilters();
    this.pushedFilterBytes = result.pushed();
    return result.postScan();
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public boolean pushLimit(int limit) {
    // DataFusion enforces the limit exactly (df.limit after filters), and a
    // limited plan coalesces to a single output partition, so the total row
    // count is bounded. Report it as fully handled.
    this.limit = limit;
    return true;
  }

  @Override
  public Scan build() {
    ScanRequest.Builder request = ScanRequest.newBuilder();
    for (String name : requiredSchema.fieldNames()) {
      request.addProjection(name);
    }
    for (byte[] filter : pushedFilterBytes) {
      request.addFilters(ByteString.copyFrom(filter));
    }
    if (limit >= 0) {
      request.setLimit(limit);
    }
    return new DatafusionScanImpl(provider, config, request.build().toByteArray(), requiredSchema);
  }
}
