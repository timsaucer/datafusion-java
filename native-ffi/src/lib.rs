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

//! A plain-C scan ABI over the Arrow C Data / C Stream interface.
//!
//! This crate exposes a DataFusion [`TableProvider`](datafusion::catalog::TableProvider)
//! scan as a set of `extern "C"` entry points that speak only C types and the
//! Arrow C Data interface. There is **no JVM/JNI dependency**: the front door
//! is callable from Java (via a thin JNI shim or the JDK 22+ FFM API), but also
//! from Python (cffi/ctypes), Go (cgo), R, or another Rust crate. That is the
//! property that lets the surface live close to DataFusion proper and get
//! reviewed by a wider audience -- the request on
//! <https://github.com/apache/datafusion-java/pull/104>.
//!
//! # Shape
//!
//! Providers are *compiled into* the final cdylib ("approach A"): a consumer
//! links this crate as an `rlib`, [`register_provider`]s its builders by name,
//! and the `df_scan_*` symbols are exported from the resulting shared library.
//! The data plane never crosses as serialized batches -- each scanned
//! partition is handed back as a standard `FFI_ArrowArrayStream` the consumer
//! imports zero-copy.
//!
//! # The ABI
//!
//! See `include/datafusion_scan.h` for the C header. In brief:
//!
//! - [`abi::df_scan_schema`]            -- probe the output schema (Arrow C Schema)
//! - [`abi::df_scan_create`]            -- plan a scan, returns an opaque handle
//! - [`abi::df_scan_partition_count`]   -- number of output partitions
//! - [`abi::df_scan_execute_partition`] -- one partition  -> Arrow C Stream
//! - [`abi::df_scan_execute`]           -- whole plan      -> Arrow C Stream
//! - [`abi::df_scan_close`]             -- drop the handle
//! - [`abi::df_error_free`]             -- free an error string
//! - [`abi::df_scan_abi_version`]       -- ABI major version for compatibility
//!
//! Every fallible call returns `0` on success and a nonzero
//! [`error::DfStatus`] code on failure, setting `*out_err` to a malloc'd,
//! NUL-terminated message the caller frees with `df_error_free`.

pub mod abi;
pub mod error;
pub mod ffi_types;
pub mod reader;
pub mod registry;
pub mod runtime;
pub mod scan;

/// Generated protobuf types for the scan config / request wire formats
/// (`proto/scan_config.proto`, `proto/scan_request.proto`). The `ScanConfig`
/// blob is decoded by provider builders; `ScanRequest` is the engine-side
/// staging object exploded into the C call's typed arguments.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/datafusion_java.rs"));
}

#[cfg(feature = "demo-providers")]
pub mod demo;

pub use registry::register_provider;

/// Major version of this ABI. Bumped on any breaking change to a `df_scan_*`
/// signature or to the meaning of its arguments. Consumers compare against the
/// value they were built for via [`abi::df_scan_abi_version`].
pub const ABI_VERSION: u64 = 1;
