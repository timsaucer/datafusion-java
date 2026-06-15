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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.datafusion.protobuf.ScanRequest;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * Unit-level proof that the scan builder encodes pushdown into the ScanRequest, isolated from
 * Spark's own limit/filter handling (which would mask whether we pushed anything).
 */
class DatafusionScanBuilderTest {

  private static final StructType SCHEMA =
      new StructType().add("id", DataTypes.LongType).add("name", DataTypes.StringType);

  private DatafusionScanBuilder builder() {
    return new DatafusionScanBuilder(SCHEMA, "datafusion.listing", new byte[0]);
  }

  private static ScanRequest decode(org.apache.spark.sql.connector.read.Scan scan)
      throws Exception {
    return ScanRequest.parseFrom(((DatafusionScanImpl) scan).scanRequestBytes());
  }

  @Test
  void pushesLimit() throws Exception {
    DatafusionScanBuilder b = builder();
    assertTrue(b.pushLimit(7), "limit should be reported as fully pushed");
    ScanRequest request = decode(b.build());
    assertTrue(request.hasLimit());
    assertEquals(7L, request.getLimit());
  }

  @Test
  void noLimitWhenNotPushed() throws Exception {
    ScanRequest request = decode(builder().build());
    assertFalse(request.hasLimit(), "limit must be unset when Spark pushes none");
  }

  @Test
  void pushesProjection() throws Exception {
    DatafusionScanBuilder b = builder();
    b.pruneColumns(new StructType().add("name", DataTypes.StringType));
    ScanRequest request = decode(b.build());
    assertEquals(List.of("name"), request.getProjectionList());
  }

  @Test
  void pushesComparisonFilter() throws Exception {
    DatafusionScanBuilder b = builder();
    Filter[] residual =
        ((SupportsPushDownFilters) b).pushFilters(new Filter[] {new GreaterThanOrEqual("id", 2L)});
    assertEquals(0, residual.length, "a translatable filter should be fully pushed");
    ScanRequest request = decode(b.build());
    assertEquals(1, request.getFiltersCount());
  }
}
