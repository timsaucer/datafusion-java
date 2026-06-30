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

//! The "custom" `TableProvider` this example exposes.
//!
//! In a real deployment this is *your* provider crate -- reading your store,
//! your format, your catalog. Here it is a tiny fixed table so the example is
//! self-contained; the only thing that matters for the integration is that it
//! is an ordinary [`datafusion::catalog::TableProvider`]. Execution is delegated
//! to an in-memory [`MemTable`]; swap [`ExampleTableProvider::scan`] for your
//! real source and the rest of the pipeline (ADBC driver, Spark connector) is
//! unchanged.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;

/// A custom provider exposing a single fixed table named [`Self::TABLE_NAME`].
#[derive(Debug)]
pub struct ExampleTableProvider {
    inner: MemTable,
}

impl ExampleTableProvider {
    /// The name the provider is registered under; the ADBC client scans this.
    pub const TABLE_NAME: &'static str = "example";

    pub fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        // Three partitions (one row each) so the table has a non-trivial output
        // partitioning -- this is what makes ADBC execute_partitions /
        // read_partition return more than one partition, exercising distributed
        // reads.
        let rows = [(1_i64, "alice"), (2, "bob"), (3, "carol")];
        let partitions: Vec<Vec<RecordBatch>> = rows
            .iter()
            .map(|(id, name)| {
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![*id])),
                        Arc::new(StringArray::from(vec![*name])),
                    ],
                )
                .expect("example record batch");
                vec![batch]
            })
            .collect();
        let inner = MemTable::try_new(schema, partitions).expect("example in-memory table");
        Self { inner }
    }
}

impl Default for ExampleTableProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableProvider for ExampleTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // A real provider builds its own ExecutionPlan here, honoring the
        // pushed projection / filters / limit. We delegate to MemTable.
        self.inner.scan(state, projection, filters, limit).await
    }
}
