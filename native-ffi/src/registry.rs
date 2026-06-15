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

//! Provider builder registry.
//!
//! "Approach A" means the providers ship compiled into the final cdylib rather
//! than being imported over an FFI. A consumer registers each builder by name
//! at startup; the C ABI selects one by that name and hands it the opaque
//! `options`/`partition` byte blobs it was given. The builder decodes those
//! however it likes (protobuf, JSON, bincode) -- the ABI stays oblivious.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use datafusion::catalog::TableProvider;

use crate::error::{DfStatus, ScanError, ScanResult};

/// Builds a provider from caller-supplied bytes.
///
/// * `options`   -- provider-level config (which table, paths, schema, ...).
/// * `partition` -- optional per-partition slice descriptor; empty for a
///   whole-table scan.
///
/// Both are opaque to the ABI; their encoding is a contract between the
/// registrant and whoever fills the bytes on the other side of the boundary.
pub type ProviderBuilder =
    fn(options: &[u8], partition: &[u8]) -> ScanResult<Arc<dyn TableProvider>>;

fn registry() -> &'static RwLock<HashMap<String, ProviderBuilder>> {
    static REGISTRY: std::sync::OnceLock<RwLock<HashMap<String, ProviderBuilder>>> =
        std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Register `builder` under `name`, replacing any previous registration.
/// Call once per provider at cdylib startup (e.g. from a `#[ctor]` or an
/// exported init function the consumer invokes).
pub fn register_provider(name: impl Into<String>, builder: ProviderBuilder) {
    registry()
        .write()
        .expect("provider registry poisoned")
        .insert(name.into(), builder);
}

/// Look up `name` and build a provider from the given bytes.
pub fn build_provider(
    name: &str,
    options: &[u8],
    partition: &[u8],
) -> ScanResult<Arc<dyn TableProvider>> {
    let builder = {
        let guard = registry().read().expect("provider registry poisoned");
        guard.get(name).copied()
    };
    match builder {
        Some(b) => b(options, partition),
        None => Err(ScanError::new(
            DfStatus::UnknownProvider,
            format!("no provider builder registered under name {name:?}"),
        )),
    }
}
