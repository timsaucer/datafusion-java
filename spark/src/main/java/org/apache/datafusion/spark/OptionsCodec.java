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

import java.util.Locale;

import org.apache.datafusion.protobuf.CsvReadOptionsProto;
import org.apache.datafusion.protobuf.ListingSource;
import org.apache.datafusion.protobuf.NdJsonReadOptionsProto;
import org.apache.datafusion.protobuf.ParquetReadOptionsProto;
import org.apache.datafusion.protobuf.ScanConfig;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Translates Spark data-source options into a {@code ScanConfig} for the {@code datafusion.listing}
 * provider.
 *
 * <p>Recognized options: {@code path} (required), {@code format} ({@code csv|parquet|json}, default
 * inferred from the path extension then {@code csv}), and for CSV {@code header} (default true) and
 * {@code delimiter} (default {@code ,}).
 */
final class OptionsCodec {

  static final String PROVIDER = "datafusion.listing";

  private OptionsCodec() {}

  /** The provider name plus the serialized ScanConfig the listing builder decodes. */
  record Source(String provider, byte[] config) {}

  static Source fromOptions(CaseInsensitiveStringMap options) {
    String path = options.get("path");
    if (path == null || path.isEmpty()) {
      throw new IllegalArgumentException("the 'datafusion' source requires a 'path' option");
    }
    String format = options.containsKey("format") ? options.get("format") : inferFormat(path);

    ListingSource.Builder listing = ListingSource.newBuilder().addPaths(path);
    switch (format.toLowerCase(Locale.ROOT)) {
      case "csv" ->
          listing.setCsv(
              CsvReadOptionsProto.newBuilder()
                  .setHasHeader(options.getBoolean("header", true))
                  .setDelimiter(delimiter(options))
                  .setQuote('"')
                  .setFileExtension(".csv")
                  .build());
      case "parquet" ->
          listing.setParquet(
              ParquetReadOptionsProto.newBuilder().setFileExtension(".parquet").build());
      case "json" ->
          listing.setJson(NdJsonReadOptionsProto.newBuilder().setFileExtension(".json").build());
      default -> throw new IllegalArgumentException("unsupported format: " + format);
    }

    byte[] config =
        ScanConfig.newBuilder()
            .setProvider(PROVIDER)
            .setListing(listing.build())
            .build()
            .toByteArray();
    return new Source(PROVIDER, config);
  }

  private static int delimiter(CaseInsensitiveStringMap options) {
    String d = options.containsKey("delimiter") ? options.get("delimiter") : ",";
    if (d.length() != 1) {
      throw new IllegalArgumentException("delimiter must be a single character, got: " + d);
    }
    return d.charAt(0);
  }

  private static String inferFormat(String path) {
    String lower = path.toLowerCase(Locale.ROOT);
    if (lower.endsWith(".parquet")) {
      return "parquet";
    }
    if (lower.endsWith(".json")) {
      return "json";
    }
    return "csv";
  }
}
