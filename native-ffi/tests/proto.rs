// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Confirms the generated scan-config / scan-request types encode and decode,
//! including a per-format read-option message embedded through the source
//! oneof -- i.e. the imports across `proto/*.proto` resolved at build time.

use datafusion_scan_ffi::proto::{
    listing_source, scan_config, CsvReadOptionsProto, ListingSource, ScanConfig, ScanRequest,
};
use prost::Message;

#[test]
fn scan_config_with_listing_source_roundtrips() {
    let config = ScanConfig {
        provider: "datafusion.listing".to_string(),
        source: Some(scan_config::Source::Listing(ListingSource {
            paths: vec!["s3://bucket/data/".to_string()],
            schema_ipc: None,
            format: Some(listing_source::Format::Csv(CsvReadOptionsProto {
                has_header: true,
                delimiter: b',' as u32,
                quote: b'"' as u32,
                file_extension: ".csv".to_string(),
                ..Default::default()
            })),
        })),
    };

    let bytes = config.encode_to_vec();
    let decoded = ScanConfig::decode(bytes.as_slice()).expect("decode ScanConfig");

    assert_eq!(decoded.provider, "datafusion.listing");
    match decoded.source {
        Some(scan_config::Source::Listing(l)) => {
            assert_eq!(l.paths, vec!["s3://bucket/data/".to_string()]);
            match l.format {
                Some(listing_source::Format::Csv(c)) => {
                    assert!(c.has_header);
                    assert_eq!(c.delimiter, b',' as u32);
                }
                other => panic!("expected CSV format, got {other:?}"),
            }
        }
        other => panic!("expected listing source, got {other:?}"),
    }
}

#[test]
fn scan_request_roundtrips() {
    let req = ScanRequest {
        projection: vec!["id".to_string(), "name".to_string()],
        filters: vec![vec![1, 2, 3], vec![4, 5]],
        limit: Some(100),
        target_partitions: 8,
        batch_size: 0,
        config_overrides: [(
            "datafusion.execution.parquet.pushdown_filters".to_string(),
            "true".to_string(),
        )]
        .into_iter()
        .collect(),
    };

    let bytes = req.encode_to_vec();
    let decoded = ScanRequest::decode(bytes.as_slice()).expect("decode ScanRequest");

    assert_eq!(decoded.projection, vec!["id", "name"]);
    assert_eq!(decoded.filters.len(), 2);
    assert_eq!(decoded.limit, Some(100));
    assert_eq!(decoded.target_partitions, 8);
    assert_eq!(
        decoded
            .config_overrides
            .get("datafusion.execution.parquet.pushdown_filters")
            .map(String::as_str),
        Some("true")
    );
}
