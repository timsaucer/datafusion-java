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

//! Error model for the C ABI.
//!
//! Rust-internal code works with [`ScanError`]; the `extern "C"` layer turns it
//! into an `i32` [`DfStatus`] return plus a heap-allocated message string. No
//! Rust error type ever crosses the boundary -- only a code and UTF-8 bytes.

use std::ffi::{c_char, CString};
use std::os::raw::c_int;

use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;

/// Status codes returned by every fallible `df_scan_*` call. `0` is success;
/// the rest classify the failure coarsely so a consumer can branch without
/// parsing the message. Stable across an `ABI_VERSION`.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DfStatus {
    Ok = 0,
    /// A required pointer argument was null, or a length/index was invalid.
    InvalidArgument = 1,
    /// `provider` is not a registered builder name.
    UnknownProvider = 2,
    /// The provider builder itself failed.
    ProviderBuild = 3,
    /// Planning failed (projection, filter decode, physical planning).
    Planning = 4,
    /// Stream execution setup failed.
    Execution = 5,
    /// A Rust panic was caught at the boundary.
    Panic = 6,
    /// Anything not covered above.
    Internal = 7,
}

/// Internal error carrying a status class and a human-readable message.
#[derive(Debug)]
pub struct ScanError {
    pub status: DfStatus,
    pub message: String,
}

impl ScanError {
    pub fn new(status: DfStatus, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(DfStatus::InvalidArgument, message)
    }
}

impl From<DataFusionError> for ScanError {
    fn from(e: DataFusionError) -> Self {
        Self::new(DfStatus::Planning, e.to_string())
    }
}

impl From<ArrowError> for ScanError {
    fn from(e: ArrowError) -> Self {
        Self::new(DfStatus::Internal, e.to_string())
    }
}

impl From<prost::DecodeError> for ScanError {
    fn from(e: prost::DecodeError) -> Self {
        Self::new(
            DfStatus::Planning,
            format!("failed to decode pushed filter as LogicalExprNode: {e}"),
        )
    }
}

pub type ScanResult<T> = Result<T, ScanError>;

/// Write `err`'s message into `*out_err` as a freshly allocated,
/// NUL-terminated C string (freed by the caller via `df_error_free`) and
/// return its status code as `c_int`. `out_err` may be null, in which case the
/// message is dropped and only the code is returned.
///
/// # Safety
/// `out_err` must be null or point to a writable `*mut c_char`.
pub unsafe fn report(out_err: *mut *mut c_char, err: ScanError) -> c_int {
    if !out_err.is_null() {
        // NUL bytes in the message would truncate it; replace defensively.
        let sanitized = err.message.replace('\0', "\u{fffd}");
        match CString::new(sanitized) {
            Ok(c) => *out_err = c.into_raw(),
            Err(_) => *out_err = std::ptr::null_mut(),
        }
    }
    err.status as c_int
}

/// Collapse a `ScanResult<()>` into a status code, reporting any error through
/// `out_err`.
///
/// # Safety
/// See [`report`].
pub unsafe fn finish(out_err: *mut *mut c_char, result: ScanResult<()>) -> c_int {
    match result {
        Ok(()) => DfStatus::Ok as c_int,
        Err(e) => report(out_err, e),
    }
}
