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

//! Exercises the plain-C ABI exactly as a foreign consumer would: call the
//! `df_scan_*` entry points with C structs, hand a caller-allocated
//! `FFI_ArrowArrayStream` across the boundary, then import it back through the
//! Arrow C Stream interface (`ArrowArrayStreamReader`) -- the Rust analogue of
//! arrow-java's `Data.importArrayStream`. No JVM involved.

use std::ffi::{c_char, CStr};
use std::ptr;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::ffi::FFI_ArrowSchema;
use datafusion::arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};

use datafusion_scan_ffi::abi::{
    df_error_free, df_scan_abi_version, df_scan_close, df_scan_create, df_scan_execute,
    df_scan_execute_partition, df_scan_partition_count, df_scan_schema, DfScanHandle,
};
use datafusion_scan_ffi::ffi_types::{DfBytes, DfStr};
use datafusion_scan_ffi::{demo, ABI_VERSION};

fn provider() -> DfStr {
    DfStr {
        ptr: demo::NAME.as_ptr(),
        len: demo::NAME.len(),
    }
}

const EMPTY_BYTES: DfBytes = DfBytes {
    ptr: ptr::null(),
    len: 0,
};

/// Pull an err string (if any) for assertions, freeing it.
unsafe fn take_err(err: *mut c_char) -> Option<String> {
    if err.is_null() {
        None
    } else {
        let s = CStr::from_ptr(err).to_string_lossy().into_owned();
        df_error_free(err);
        Some(s)
    }
}

#[test]
fn abi_version_matches() {
    assert_eq!(df_scan_abi_version(), ABI_VERSION);
}

#[test]
fn schema_probe_returns_provider_schema() {
    demo::register();
    let mut out = FFI_ArrowSchema::empty();
    let mut err: *mut c_char = ptr::null_mut();
    let status =
        unsafe { df_scan_schema(provider(), EMPTY_BYTES, EMPTY_BYTES, &mut out, &mut err) };
    assert_eq!(status, 0, "err: {:?}", unsafe { take_err(err) });

    let schema =
        datafusion::arrow::datatypes::Schema::try_from(&out).expect("import FFI_ArrowSchema");
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["id", "name"]);
}

#[test]
fn unknown_provider_reports_status_and_message() {
    let bad = DfStr {
        ptr: b"nope".as_ptr(),
        len: 4,
    };
    let mut out = FFI_ArrowSchema::empty();
    let mut err: *mut c_char = ptr::null_mut();
    let status = unsafe { df_scan_schema(bad, EMPTY_BYTES, EMPTY_BYTES, &mut out, &mut err) };
    assert_eq!(status, 2 /* DF_UNKNOWN_PROVIDER */);
    let msg = unsafe { take_err(err) }.expect("error message");
    assert!(msg.contains("nope"), "msg was: {msg}");
}

#[test]
fn create_reports_two_partitions() {
    demo::register();
    let handle = create_full_scan();
    let mut count = 0i32;
    let mut err: *mut c_char = ptr::null_mut();
    let status = unsafe { df_scan_partition_count(handle, &mut count, &mut err) };
    assert_eq!(status, 0, "err: {:?}", unsafe { take_err(err) });
    assert_eq!(count, 2, "demo provider has two partitions");
    unsafe { df_scan_close(handle) };
}

#[test]
fn execute_partition_roundtrips_arrow_c_stream() {
    demo::register();
    let handle = create_full_scan();

    // Sum `id` across both partitions by importing each stream back through
    // the Arrow C Stream interface, the way a foreign consumer would.
    let mut total: i64 = 0;
    let mut rows = 0usize;
    for partition in 0..2 {
        let mut stream = FFI_ArrowArrayStream::empty();
        let mut err: *mut c_char = ptr::null_mut();
        let status = unsafe { df_scan_execute_partition(handle, partition, &mut stream, &mut err) };
        assert_eq!(status, 0, "err: {:?}", unsafe { take_err(err) });

        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut stream) }
            .expect("import FFI_ArrowArrayStream");
        for batch in reader {
            let batch = batch.expect("batch");
            rows += batch.num_rows();
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column is Int64");
            total += ids.values().iter().sum::<i64>();
        }
    }

    assert_eq!(rows, 5, "3 + 2 rows across the two partitions");
    assert_eq!(total, 1 + 2 + 3 + 4 + 5);
    unsafe { df_scan_close(handle) };
}

#[test]
fn limit_caps_row_count() {
    demo::register();
    // demo provider has 5 rows across two partitions; cap at 2.
    let mut handle: *mut DfScanHandle = ptr::null_mut();
    let mut err: *mut c_char = ptr::null_mut();
    let status = unsafe {
        df_scan_create(
            provider(),
            EMPTY_BYTES,
            EMPTY_BYTES,
            0,
            0,
            2, // limit
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

    // Read the whole plan; the limit must hold across partitions.
    let mut stream = FFI_ArrowArrayStream::empty();
    let mut err2: *mut c_char = ptr::null_mut();
    assert_eq!(
        unsafe { df_scan_execute(handle, &mut stream, &mut err2) },
        0,
        "err: {:?}",
        unsafe { take_err(err2) }
    );
    let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut stream) }.expect("import");
    let rows: usize = reader.map(|b| b.expect("batch").num_rows()).sum();
    assert_eq!(rows, 2, "limit should cap the scan at 2 rows");

    unsafe { df_scan_close(handle) };
}

#[test]
fn close_is_null_safe() {
    unsafe { df_scan_close(ptr::null_mut()) };
}

/// Plan a full scan (no projection / filters) over the demo provider.
fn create_full_scan() -> *mut DfScanHandle {
    let mut handle: *mut DfScanHandle = ptr::null_mut();
    let mut err: *mut c_char = ptr::null_mut();
    let status = unsafe {
        df_scan_create(
            provider(),
            EMPTY_BYTES,
            EMPTY_BYTES,
            0,
            0,
            -1,
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
    handle
}
