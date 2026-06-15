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

//! A reference in-memory provider builder, gated behind the `demo-providers`
//! feature. Registered under `datafusion.memory`; the `options` bytes are
//! ignored. Used by the round-trip tests and as a minimal example of what a
//! real consumer's builder looks like.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

use crate::error::{DfStatus, ScanError, ScanResult};
use crate::registry::register_provider;

/// Registered builder name for the demo provider.
pub const NAME: &str = "datafusion.memory";

/// Register the demo provider. Call once at startup.
pub fn register() {
    register_provider(NAME, build);
}

/// Two-column (`id: Int64`, `name: Utf8`), two-batch in-memory table across
/// two partitions, so partition-count behavior is observable.
fn build(
    _ctx: &SessionContext,
    _options: &[u8],
    _partition: &[u8],
) -> ScanResult<Arc<dyn TableProvider>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let batch = |ids: Vec<i64>, names: Vec<&str>| -> ScanResult<RecordBatch> {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .map_err(ScanError::from)
    };

    let p0 = batch(vec![1, 2, 3], vec!["a", "b", "c"])?;
    let p1 = batch(vec![4, 5], vec!["d", "e"])?;

    MemTable::try_new(schema, vec![vec![p0], vec![p1]])
        .map(|t| Arc::new(t) as Arc<dyn TableProvider>)
        .map_err(|e| ScanError::new(DfStatus::ProviderBuild, e.to_string()))
}
