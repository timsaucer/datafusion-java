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

//! The plain-C front door: `extern "C"` entry points over C and Arrow C types.
//!
//! No `JNIEnv`, no JVM types, no name mangling -- the exported symbols are
//! `df_scan_*` / `df_error_*` and the only "rich" types that cross are the
//! standard Arrow C Data (`ArrowSchema`) and C Stream (`ArrowArrayStream`)
//! structs. A Java consumer reaches these through a ~2-method JNI shim or the
//! JDK 22+ FFM API; Python/Go/R/Rust reach them directly.
//!
//! Convention: every fallible call returns `0` on success and a nonzero
//! [`DfStatus`](crate::error::DfStatus) on failure, writing a malloc'd message
//! to `*out_err` (freed via [`df_error_free`]). Each is wrapped in
//! `catch_unwind` so a Rust panic becomes [`DfStatus::Panic`] instead of
//! unwinding across the C boundary (UB).

use std::ffi::c_char;
use std::os::raw::c_int;
use std::panic::{catch_unwind, AssertUnwindSafe};

use datafusion::arrow::ffi::FFI_ArrowSchema;
use datafusion::arrow::ffi_stream::FFI_ArrowArrayStream;

use crate::error::{finish, report, DfStatus, ScanError, ScanResult};
use crate::ffi_types::{array, DfBytes, DfKeyValue, DfStr};
use crate::reader::panic_message;
use crate::scan::{self, ScanHandle, ScanRequest};

/// Opaque handle to a planned scan. Created by [`df_scan_create`], freed by
/// [`df_scan_close`]. Never dereferenced by the consumer.
pub struct DfScanHandle {
    inner: ScanHandle,
}

/// Run `body`, turning a caught panic into a [`DfStatus::Panic`] status.
///
/// # Safety
/// `out_err` must be null or a writable `*mut *mut c_char`.
unsafe fn guard(out_err: *mut *mut c_char, body: impl FnOnce() -> ScanResult<()>) -> c_int {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(result) => finish(out_err, result),
        Err(p) => report(
            out_err,
            ScanError::new(
                DfStatus::Panic,
                format!("panic in datafusion-scan-ffi: {}", panic_message(&p)),
            ),
        ),
    }
}

/// Major version of the ABI. A consumer compares this against the value it was
/// compiled for before calling anything else.
#[no_mangle]
pub extern "C" fn df_scan_abi_version() -> u64 {
    crate::ABI_VERSION
}

/// Free an error string previously written to an `out_err` argument. Safe to
/// call with null.
///
/// # Safety
/// `err` must be null or a pointer previously returned through `out_err` by
/// one of the `df_scan_*` calls, and must not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn df_error_free(err: *mut c_char) {
    if !err.is_null() {
        drop(std::ffi::CString::from_raw(err));
    }
}

/// Probe a provider's output schema, writing an Arrow C Schema into the
/// caller-allocated `out_schema`.
///
/// # Safety
/// All pointer args follow the documented `(ptr, len)` borrow contract;
/// `out_schema` must point to a writable, uninitialized `ArrowSchema`.
#[no_mangle]
pub unsafe extern "C" fn df_scan_schema(
    provider: DfStr,
    options: DfBytes,
    partition: DfBytes,
    out_schema: *mut FFI_ArrowSchema,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(out_err, || {
        if out_schema.is_null() {
            return Err(ScanError::invalid_argument("out_schema is null"));
        }
        let name = provider.as_str()?;
        let schema = scan::schema(name, options.as_slice(), partition.as_slice())?;
        let ffi = FFI_ArrowSchema::try_from(schema.as_ref())?;
        std::ptr::write(out_schema, ffi);
        Ok(())
    })
}

/// Plan a scan. On success writes an owned [`DfScanHandle`] pointer to
/// `*out_handle`; the caller must release it with [`df_scan_close`].
///
/// `config_keys`/`config_values` ... here folded into a single
/// `config_overrides` array of [`DfKeyValue`]. `projection` is an array of
/// column-name [`DfStr`]s (empty selects all). `filters` is an array of
/// serialized `datafusion.LogicalExprNode` [`DfBytes`].
///
/// # Safety
/// Array args follow the `(ptr, len)` borrow contract; `out_handle` must be a
/// writable `*mut *mut DfScanHandle`.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn df_scan_create(
    provider: DfStr,
    options: DfBytes,
    partition: DfBytes,
    target_partitions: c_int,
    batch_size: c_int,
    config_overrides: *const DfKeyValue,
    config_overrides_len: usize,
    projection: *const DfStr,
    projection_len: usize,
    filters: *const DfBytes,
    filters_len: usize,
    out_handle: *mut *mut DfScanHandle,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(out_err, || {
        if out_handle.is_null() {
            return Err(ScanError::invalid_argument("out_handle is null"));
        }
        let provider = provider.as_str()?;

        let mut overrides = Vec::with_capacity(config_overrides_len);
        for kv in array(config_overrides, config_overrides_len) {
            overrides.push((kv.key.as_str()?.to_string(), kv.value.as_str()?.to_string()));
        }
        let mut cols = Vec::with_capacity(projection_len);
        for s in array(projection, projection_len) {
            cols.push(s.as_str()?.to_string());
        }
        let mut filter_bytes = Vec::with_capacity(filters_len);
        for b in array(filters, filters_len) {
            filter_bytes.push(b.as_slice().to_vec());
        }

        let handle = scan::create(ScanRequest {
            provider,
            options: options.as_slice(),
            partition: partition.as_slice(),
            target_partitions,
            batch_size,
            config_overrides: overrides,
            projection: cols,
            filters: filter_bytes,
        })?;

        let boxed = Box::new(DfScanHandle { inner: handle });
        std::ptr::write(out_handle, Box::into_raw(boxed));
        Ok(())
    })
}

/// Number of output partitions of the planned scan.
///
/// # Safety
/// `handle` must be a live pointer from [`df_scan_create`]; `out_count` must be
/// writable.
#[no_mangle]
pub unsafe extern "C" fn df_scan_partition_count(
    handle: *const DfScanHandle,
    out_count: *mut c_int,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(out_err, || {
        let h = handle
            .as_ref()
            .ok_or_else(|| ScanError::invalid_argument("scan handle is null"))?;
        if out_count.is_null() {
            return Err(ScanError::invalid_argument("out_count is null"));
        }
        std::ptr::write(out_count, h.inner.partition_count() as c_int);
        Ok(())
    })
}

/// Execute one plan partition, writing an `FFI_ArrowArrayStream` into the
/// caller-allocated `out_stream`. The consumer imports it with its Arrow C
/// Stream importer (e.g. arrow-java `Data.importArrayStream`).
///
/// # Safety
/// `handle` live; `out_stream` points to a writable, uninitialized
/// `ArrowArrayStream`.
#[no_mangle]
pub unsafe extern "C" fn df_scan_execute_partition(
    handle: *const DfScanHandle,
    partition: c_int,
    out_stream: *mut FFI_ArrowArrayStream,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(out_err, || {
        let h = handle
            .as_ref()
            .ok_or_else(|| ScanError::invalid_argument("scan handle is null"))?;
        if out_stream.is_null() {
            return Err(ScanError::invalid_argument("out_stream is null"));
        }
        if partition < 0 {
            return Err(ScanError::invalid_argument("partition index is negative"));
        }
        let reader = h.inner.execute_partition(partition as usize)?;
        let ffi = FFI_ArrowArrayStream::new(Box::new(reader));
        std::ptr::write(out_stream, ffi);
        Ok(())
    })
}

/// Execute the whole plan as a single coalesced stream.
///
/// # Safety
/// As [`df_scan_execute_partition`].
#[no_mangle]
pub unsafe extern "C" fn df_scan_execute(
    handle: *const DfScanHandle,
    out_stream: *mut FFI_ArrowArrayStream,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(out_err, || {
        let h = handle
            .as_ref()
            .ok_or_else(|| ScanError::invalid_argument("scan handle is null"))?;
        if out_stream.is_null() {
            return Err(ScanError::invalid_argument("out_stream is null"));
        }
        let reader = h.inner.execute_all()?;
        let ffi = FFI_ArrowArrayStream::new(Box::new(reader));
        std::ptr::write(out_stream, ffi);
        Ok(())
    })
}

/// Drop a planned scan. Must not race an in-flight execute on the same handle;
/// the consumer is responsible for that ordering. Safe to call with null.
///
/// # Safety
/// `handle` must be null or a live pointer from [`df_scan_create`], not used
/// afterwards.
#[no_mangle]
pub unsafe extern "C" fn df_scan_close(handle: *mut DfScanHandle) {
    if !handle.is_null() {
        drop(Box::from_raw(handle));
    }
}
