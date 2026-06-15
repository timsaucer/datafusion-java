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

//! The cdylib-wide Tokio runtime.
//!
//! DataFusion planning and execution are async; this ABI is synchronous, so
//! every call that awaits does so through this runtime. Statically linked into
//! whatever cdylib embeds this crate, so it is a per-cdylib singleton -- two
//! libraries loaded in one process get independent runtimes and cannot collide.
//!
//! This mirrors `datafusion-jni-common`'s runtime but is deliberately
//! duplicated here so the C ABI carries no dependency on the JNI crate.

use std::sync::OnceLock;

use tokio::runtime::{Handle, Runtime};

static RT: OnceLock<Runtime> = OnceLock::new();

/// The shared multi-thread Tokio runtime, created on first use.
pub fn runtime() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("failed to create Tokio runtime"))
}

/// Handle to [`runtime`], for `block_on` / `enter`.
pub fn handle() -> &'static Handle {
    runtime().handle()
}
