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

import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.datafusion.scan.DatafusionScan;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Entry point for the {@code datafusion} Spark data source.
 *
 * <p>Registered via {@code DataSourceRegister} so {@code
 * spark.read.format("datafusion").option("path", ...).load()} resolves here. Options are decoded
 * into a {@code ScanConfig} ({@link OptionsCodec}); the schema is probed once, on the driver,
 * through {@link DatafusionScan#schema}.
 */
public final class DatafusionTableProvider implements TableProvider, DataSourceRegister {

  @Override
  public String shortName() {
    return "datafusion";
  }

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    OptionsCodec.Source source = OptionsCodec.fromOptions(options);
    try (BufferAllocator allocator = new RootAllocator()) {
      Schema arrow = DatafusionScan.schema(allocator, source.provider(), source.config());
      return SchemaConverter.toSparkSchema(arrow);
    }
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    OptionsCodec.Source source = OptionsCodec.fromOptions(new CaseInsensitiveStringMap(properties));
    return new DatafusionTable(schema, source.provider(), source.config());
  }

  @Override
  public boolean supportsExternalMetadata() {
    return false;
  }
}
