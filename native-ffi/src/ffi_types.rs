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

//! Borrowed C views passed *into* the ABI.
//!
//! These are non-owning `(ptr, len)` pairs: the caller owns the memory and
//! keeps it valid for the duration of the call. Nothing here is allocated or
//! freed by Rust. Using explicit `(ptr, len)` slices (rather than
//! NUL-terminated strings) means the surface is FFM-friendly and binary-safe.

use std::slice;

use crate::error::{ScanError, ScanResult};

/// A borrowed UTF-8 string slice. Not NUL-terminated.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DfStr {
    pub ptr: *const u8,
    pub len: usize,
}

/// A borrowed byte slice.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DfBytes {
    pub ptr: *const u8,
    pub len: usize,
}

/// A borrowed `(key, value)` UTF-8 pair, for session config overrides.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DfKeyValue {
    pub key: DfStr,
    pub value: DfStr,
}

impl DfStr {
    /// # Safety
    /// `ptr` must be null or point to `len` valid bytes of UTF-8 that stay
    /// alive for the borrow.
    pub unsafe fn as_str(&self) -> ScanResult<&str> {
        let bytes = self.as_bytes();
        std::str::from_utf8(bytes)
            .map_err(|e| ScanError::invalid_argument(format!("argument is not valid UTF-8: {e}")))
    }

    /// # Safety
    /// See [`DfStr::as_str`].
    pub unsafe fn as_bytes(&self) -> &[u8] {
        if self.ptr.is_null() || self.len == 0 {
            &[]
        } else {
            slice::from_raw_parts(self.ptr, self.len)
        }
    }
}

impl DfBytes {
    /// # Safety
    /// `ptr` must be null or point to `len` valid bytes alive for the borrow.
    pub unsafe fn as_slice(&self) -> &[u8] {
        if self.ptr.is_null() || self.len == 0 {
            &[]
        } else {
            slice::from_raw_parts(self.ptr, self.len)
        }
    }
}

/// View a `(ptr, len)` array argument as a slice, treating null+0 as empty.
///
/// # Safety
/// `ptr` must be null or point to `len` valid `T` for the borrow.
pub unsafe fn array<'a, T>(ptr: *const T, len: usize) -> &'a [T] {
    if ptr.is_null() || len == 0 {
        &[]
    } else {
        slice::from_raw_parts(ptr, len)
    }
}
