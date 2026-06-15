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

//! Thin JNI shim over the plain-C scan core (`datafusion-scan-ffi`).
//!
//! This is the JVM's path to the scan ABI. It is deliberately minimal: it
//! marshals Java arguments (a `String` provider name and two `byte[]` blobs)
//! into the in-process scan core, hands back an opaque handle as a `jlong`,
//! and -- for the data plane -- writes a standard `FFI_ArrowArrayStream` (or
//! `FFI_ArrowSchema`) into the address arrow-java allocated. **No Arrow data
//! crosses the JNI boundary**: batches flow through the Arrow C Stream
//! interface, which arrow-java imports with `Data.importArrayStream`.
//!
//! Everything here mirrors `core`'s existing `DataFrame` collect path; the only
//! new ABI is the handful of `Java_org_apache_datafusion_scan_NativeScan_*`
//! entry points below. Non-Java consumers use the `df_scan_*` C symbols
//! exported by `datafusion-scan-ffi` instead; this crate is purely the JVM
//! adapter.

use std::sync::OnceLock;

use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use datafusion_scan_ffi::proto::ScanRequest as ProtoScanRequest;
use datafusion_scan_ffi::scan::{self, ScanHandle, ScanRequest};
use datafusion_scan_ffi::{demo, listing};
use jni::objects::{JByteArray, JClass, JString};
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use prost::Message;

/// Register the in-tree providers exactly once. The shim is the registration
/// point for the JVM build; a non-Java embedder registers its own.
fn ensure_registered() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        listing::register();
        demo::register();
    });
}

/// Run `body`; on `Err`, throw a Java `RuntimeException` and return `default`.
/// Mirrors the project's existing `try_unwrap_or_throw` pattern.
fn try_or_throw<T>(
    env: &mut JNIEnv,
    default: T,
    body: impl FnOnce(&mut JNIEnv) -> Result<T, String>,
) -> T {
    match body(env) {
        Ok(value) => value,
        Err(message) => {
            // If throwing fails there is nothing more we can do; the default is
            // still returned so we don't leave the stack in a bad state.
            let _ = env.throw_new("java/lang/RuntimeException", message);
            default
        }
    }
}

fn read_bytes(env: &mut JNIEnv, arr: &JByteArray) -> Result<Vec<u8>, String> {
    if arr.is_null() {
        Ok(Vec::new())
    } else {
        env.convert_byte_array(arr).map_err(|e| e.to_string())
    }
}

fn read_string(env: &mut JNIEnv, s: &JString) -> Result<String, String> {
    env.get_string(s).map(Into::into).map_err(|e| e.to_string())
}

/// Decode the engine's `ScanRequest` blob into the scan core's request,
/// borrowing the provider name and config bytes. Empty blob -> no pushdown.
fn build_request<'a>(
    provider: &'a str,
    config: &'a [u8],
    scan_request: &[u8],
) -> Result<ScanRequest<'a>, String> {
    let req = if scan_request.is_empty() {
        ProtoScanRequest::default()
    } else {
        ProtoScanRequest::decode(scan_request)
            .map_err(|e| format!("failed to decode ScanRequest: {e}"))?
    };
    // NOTE: `req.limit` is carried in the proto but not yet applied by the scan
    // core or the C ABI; wire it through in a follow-up so both consumers agree.
    Ok(ScanRequest {
        provider,
        options: config,
        partition: &[],
        target_partitions: req.target_partitions,
        batch_size: req.batch_size,
        config_overrides: req.config_overrides.into_iter().collect(),
        projection: req.projection,
        filters: req.filters,
    })
}

/// Probe a provider's output schema, writing an `FFI_ArrowSchema` into the
/// arrow-java-allocated `ArrowSchema` at `schema_addr`.
#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_scan_NativeScan_providerSchema<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    provider: JString<'local>,
    config: JByteArray<'local>,
    schema_addr: jlong,
) {
    ensure_registered();
    try_or_throw(&mut env, (), |env| {
        if schema_addr == 0 {
            return Err("schema address is null".to_string());
        }
        let provider = read_string(env, &provider)?;
        let config = read_bytes(env, &config)?;
        let schema = scan::schema(&provider, &config, &[]).map_err(|e| e.message)?;
        let ffi = FFI_ArrowSchema::try_from(schema.as_ref()).map_err(|e| e.to_string())?;
        // SAFETY: arrow-java allocated an empty ArrowSchema at this address.
        unsafe { std::ptr::write(schema_addr as *mut FFI_ArrowSchema, ffi) };
        Ok(())
    })
}

/// Plan a scan. Returns an opaque handle (boxed [`ScanHandle`] pointer) as a
/// `jlong`, or 0 after throwing on error. Release with `closeScan`.
#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_scan_NativeScan_createScan<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    provider: JString<'local>,
    config: JByteArray<'local>,
    scan_request: JByteArray<'local>,
) -> jlong {
    ensure_registered();
    try_or_throw(&mut env, 0, |env| {
        let provider = read_string(env, &provider)?;
        let config = read_bytes(env, &config)?;
        let scan_request = read_bytes(env, &scan_request)?;
        let request = build_request(&provider, &config, &scan_request)?;
        let handle = scan::create(request).map_err(|e| e.message)?;
        Ok(Box::into_raw(Box::new(handle)) as jlong)
    })
}

/// Output partition count of a planned scan.
#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_scan_NativeScan_partitionCount<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jint {
    try_or_throw(&mut env, 0, |_env| {
        let scan = handle_ref(handle)?;
        Ok(scan.partition_count() as jint)
    })
}

/// Execute one partition, writing an `FFI_ArrowArrayStream` into the
/// arrow-java-allocated `ArrowArrayStream` at `stream_addr`.
#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_scan_NativeScan_executeStreamPartition<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    partition: jint,
    stream_addr: jlong,
) {
    try_or_throw(&mut env, (), |_env| {
        if partition < 0 {
            return Err("partition index is negative".to_string());
        }
        let scan = handle_ref(handle)?;
        let reader = scan
            .execute_partition(partition as usize)
            .map_err(|e| e.message)?;
        write_stream(stream_addr, FFI_ArrowArrayStream::new(Box::new(reader)))
    })
}

/// Execute the whole plan as a single coalesced stream.
#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_scan_NativeScan_executeStream<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    stream_addr: jlong,
) {
    try_or_throw(&mut env, (), |_env| {
        let scan = handle_ref(handle)?;
        let reader = scan.execute_all().map_err(|e| e.message)?;
        write_stream(stream_addr, FFI_ArrowArrayStream::new(Box::new(reader)))
    })
}

/// Drop a planned scan. Null-safe; must not race an in-flight execute on the
/// same handle (the Java wrapper enforces this).
#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_scan_NativeScan_closeScan<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    if handle != 0 {
        // SAFETY: handle came from createScan and is not used afterwards.
        drop(unsafe { Box::from_raw(handle as *mut ScanHandle) });
    }
}

/// Borrow a [`ScanHandle`] from a `jlong`, erroring on null.
fn handle_ref<'a>(handle: jlong) -> Result<&'a ScanHandle, String> {
    if handle == 0 {
        return Err("scan handle is null".to_string());
    }
    // SAFETY: handle came from createScan and outlives this borrow.
    Ok(unsafe { &*(handle as *const ScanHandle) })
}

fn write_stream(stream_addr: jlong, ffi: FFI_ArrowArrayStream) -> Result<(), String> {
    if stream_addr == 0 {
        return Err("stream address is null".to_string());
    }
    // SAFETY: arrow-java allocated an empty ArrowArrayStream at this address.
    unsafe { std::ptr::write(stream_addr as *mut FFI_ArrowArrayStream, ffi) };
    Ok(())
}
