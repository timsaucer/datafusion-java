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

//! End-to-end test of the `datafusion.listing` provider through the plain-C
//! ABI: write a CSV, encode a ScanConfig pointing at it, scan it, and import
//! the result back through the Arrow C Stream interface -- the path a foreign
//! consumer takes. Exercises schema inference (no explicit schema supplied).

use std::ffi::{c_char, CStr};
use std::fs;
use std::process;
use std::ptr;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::ffi::FFI_ArrowSchema;
use datafusion::arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};

use datafusion_scan_ffi::abi::{
    df_error_free, df_scan_close, df_scan_create, df_scan_execute, df_scan_partition_count,
    df_scan_schema, DfScanHandle,
};
use datafusion_scan_ffi::ffi_types::{DfBytes, DfStr};
use datafusion_scan_ffi::listing;
use datafusion_scan_ffi::proto::{
    listing_source, scan_config, CsvReadOptionsProto, ListingSource, ScanConfig,
};
use prost::Message;

unsafe fn take_err(err: *mut c_char) -> Option<String> {
    if err.is_null() {
        None
    } else {
        let s = CStr::from_ptr(err).to_string_lossy().into_owned();
        df_error_free(err);
        Some(s)
    }
}

/// Write a CSV into a unique temp dir and return (dir, file path).
fn write_csv() -> (std::path::PathBuf, String) {
    let dir = std::env::temp_dir().join(format!("df-scan-ffi-{}", process::id()));
    fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("data.csv");
    fs::write(&path, "id,name\n1,a\n2,b\n3,c\n").expect("write csv");
    (dir, path.to_string_lossy().into_owned())
}

/// Encode a ScanConfig for a CSV listing source over `path`.
fn csv_config(path: &str) -> Vec<u8> {
    ScanConfig {
        provider: listing::NAME.to_string(),
        source: Some(scan_config::Source::Listing(ListingSource {
            paths: vec![path.to_string()],
            schema_ipc: None,
            format: Some(listing_source::Format::Csv(CsvReadOptionsProto {
                has_header: true,
                delimiter: b',' as u32,
                quote: b'"' as u32,
                file_extension: ".csv".to_string(),
                ..Default::default()
            })),
        })),
    }
    .encode_to_vec()
}

fn provider() -> DfStr {
    DfStr {
        ptr: listing::NAME.as_ptr(),
        len: listing::NAME.len(),
    }
}

fn options(bytes: &[u8]) -> DfBytes {
    DfBytes {
        ptr: bytes.as_ptr(),
        len: bytes.len(),
    }
}

const EMPTY: DfBytes = DfBytes {
    ptr: ptr::null(),
    len: 0,
};

#[test]
fn listing_csv_schema_is_inferred() {
    listing::register();
    let (_dir, path) = write_csv();
    let cfg = csv_config(&path);

    let mut schema = FFI_ArrowSchema::empty();
    let mut err: *mut c_char = ptr::null_mut();
    let status = unsafe { df_scan_schema(provider(), options(&cfg), EMPTY, &mut schema, &mut err) };
    assert_eq!(status, 0, "err: {:?}", unsafe { take_err(err) });

    let schema =
        datafusion::arrow::datatypes::Schema::try_from(&schema).expect("import FFI_ArrowSchema");
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["id", "name"]);
}

#[test]
fn listing_csv_scans_rows() {
    listing::register();
    let (_dir, path) = write_csv();
    let cfg = csv_config(&path);

    // Plan.
    let mut handle: *mut DfScanHandle = ptr::null_mut();
    let mut err: *mut c_char = ptr::null_mut();
    let status = unsafe {
        df_scan_create(
            provider(),
            options(&cfg),
            EMPTY,
            0,
            0,
            ptr::null(),
            0,
            ptr::null(),
            0,
            ptr::null(),
            0,
            &mut handle,
            &mut err,
        )
    };
    assert_eq!(status, 0, "err: {:?}", unsafe { take_err(err) });
    assert!(!handle.is_null());

    // Partition count is reported.
    let mut count = 0i32;
    let mut err2: *mut c_char = ptr::null_mut();
    assert_eq!(
        unsafe { df_scan_partition_count(handle, &mut count, &mut err2) },
        0
    );
    assert!(count >= 1, "expected at least one partition, got {count}");

    // Execute the whole plan as one coalesced stream and sum `id`.
    let mut stream = FFI_ArrowArrayStream::empty();
    let mut err3: *mut c_char = ptr::null_mut();
    let status = unsafe { df_scan_execute(handle, &mut stream, &mut err3) };
    assert_eq!(status, 0, "err: {:?}", unsafe { take_err(err3) });

    let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut stream) }.expect("import stream");
    let mut total: i64 = 0;
    let mut rows = 0usize;
    for batch in reader {
        let batch = batch.expect("batch");
        rows += batch.num_rows();
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        total += ids.values().iter().sum::<i64>();
    }
    assert_eq!(rows, 3);
    assert_eq!(total, 1 + 2 + 3);

    unsafe { df_scan_close(handle) };
}
