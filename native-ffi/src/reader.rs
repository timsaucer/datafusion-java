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

//! Bridge from DataFusion's async stream to the synchronous
//! [`RecordBatchReader`] that `FFI_ArrowArrayStream` pulls.

use std::panic::{catch_unwind, AssertUnwindSafe};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatchReader;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;

use crate::runtime::runtime;

/// Wraps a [`SendableRecordBatchStream`] as a [`RecordBatchReader`]. Each
/// `next()` drives one `block_on(stream.next())`, so memory stays bounded by
/// the pipeline plus a single in-flight batch.
pub struct StreamingReader {
    pub schema: SchemaRef,
    pub stream: SendableRecordBatchStream,
}

impl Iterator for StreamingReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Arrow's C Stream vtable calls this from the *consumer's* thread,
        // outside any guard. A panic unwinding across the C boundary is UB, so
        // catch it and surface as an ArrowError -- the consumer sees a normal
        // stream error (mapped to an exception on the Java side).
        let next = catch_unwind(AssertUnwindSafe(|| runtime().block_on(self.stream.next())));
        match next {
            Ok(item) => item.map(|r| r.map_err(|e| ArrowError::ExternalError(Box::new(e)))),
            Err(panic) => Some(Err(ArrowError::ExternalError(
                format!("panic in DataFusion stream: {}", panic_message(&panic)).into(),
            ))),
        }
    }
}

impl RecordBatchReader for StreamingReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Best-effort extraction of a panic payload's message.
pub fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}
