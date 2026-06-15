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

//! Planning and execution core, free of any C/JVM concerns.
//!
//! This is the JNI-free port of the logic in PR #103's `spark/bridge/src/scan.rs`:
//! build the provider, register it on a private `SessionContext` with the
//! caller-pinned config, apply the pruned projection and proto-encoded pushed
//! filters, and plan once. The resulting [`ScanHandle`] then yields one
//! independent stream per plan partition.
//!
//! Spark-specific type widening is intentionally **not** here: it is a consumer
//! concern (apply a `WideningTableProvider` decorator inside the registered
//! builder if you need it), so this core stays a faithful DataFusion scan.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::dataframe::DataFrame;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_proto::logical_plan::from_proto::parse_expr;
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
use datafusion_proto::protobuf::LogicalExprNode;
use prost::Message;

use crate::error::{DfStatus, ScanError, ScanResult};
use crate::reader::StreamingReader;
use crate::registry::build_provider;
use crate::runtime::handle;

/// Registration name of the provider on the scan's private context. Never
/// surfaces in SQL (the plan is built through the DataFrame API), so no
/// quoting/collision concern.
const SCAN_TABLE_NAME: &str = "df_scan";

/// Inputs to [`create`], decoded from the C arguments by the ABI layer.
pub struct ScanRequest<'a> {
    pub provider: &'a str,
    pub options: &'a [u8],
    pub partition: &'a [u8],
    /// `<= 0` leaves the DataFusion default.
    pub target_partitions: i32,
    /// `<= 0` leaves the DataFusion default.
    pub batch_size: i32,
    pub config_overrides: Vec<(String, String)>,
    /// Column names to project; empty selects all.
    pub projection: Vec<String>,
    /// Each entry is a serialized `datafusion.LogicalExprNode`.
    pub filters: Vec<Vec<u8>>,
}

/// A planned scan. Holds the context alive for the plan's lifetime.
pub struct ScanHandle {
    _ctx: SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
}

/// Build the provider via the registry and return its output schema, without
/// planning. Mirrors #103's `provider_schema_ipc`, but returns the live
/// `SchemaRef` (the ABI converts it to an Arrow C Schema).
pub fn schema(provider: &str, options: &[u8], partition: &[u8]) -> ScanResult<SchemaRef> {
    let provider = build_provider(provider, options, partition)?;
    Ok(provider.schema())
}

/// Build, register, project, filter, and plan exactly once.
pub fn create(req: ScanRequest<'_>) -> ScanResult<ScanHandle> {
    let provider = build_provider(req.provider, req.options, req.partition)?;

    let mut config = SessionConfig::new();
    if req.target_partitions > 0 {
        config = config.with_target_partitions(req.target_partitions as usize);
    }
    if req.batch_size > 0 {
        config = config.with_batch_size(req.batch_size as usize);
    }
    for (key, value) in &req.config_overrides {
        config.options_mut().set(key, value)?;
    }

    let ctx = SessionContext::new_with_config(config);
    ctx.register_table(SCAN_TABLE_NAME, provider)?;

    let mut df: DataFrame = handle().block_on(ctx.table(SCAN_TABLE_NAME))?;
    if !req.projection.is_empty() {
        let refs: Vec<&str> = req.projection.iter().map(String::as_str).collect();
        df = df.select_columns(&refs)?;
    }
    for bytes in &req.filters {
        let node = LogicalExprNode::decode(bytes.as_slice())?;
        // TaskContext implements FunctionRegistry; the default codec suffices
        // for the column/literal/builtin expressions a predicate translator
        // emits.
        let registry = df.task_ctx();
        let expr = parse_expr(&node, &registry, &DefaultLogicalExtensionCodec {})
            .map_err(|e| ScanError::new(DfStatus::Planning, e.to_string()))?;
        df = df.filter(expr)?;
    }

    // task_ctx() borrows df; capture before create_physical_plan consumes it.
    let task_ctx = Arc::new(df.task_ctx());
    let plan = handle().block_on(df.create_physical_plan())?;

    Ok(ScanHandle {
        _ctx: ctx,
        plan,
        task_ctx,
    })
}

impl ScanHandle {
    /// Output partition count of the planned physical plan.
    pub fn partition_count(&self) -> usize {
        self.plan
            .properties()
            .output_partitioning()
            .partition_count()
    }

    /// Open an independent reader over one plan partition. Concurrently
    /// callable across partitions: `ExecutionPlan`/`TaskContext` are
    /// `Send + Sync`, and each call only clones their `Arc`s.
    pub fn execute_partition(&self, partition: usize) -> ScanResult<StreamingReader> {
        let count = self.partition_count();
        if partition >= count {
            return Err(ScanError::new(
                DfStatus::InvalidArgument,
                format!("partition index {partition} out of range: plan has {count} partition(s)"),
            ));
        }
        let plan = Arc::clone(&self.plan);
        let task_ctx = Arc::clone(&self.task_ctx);
        let schema: SchemaRef = plan.schema();

        // execute() is synchronous but operators may tokio::spawn at
        // execute()-time (RepartitionExec et al.), needing a runtime context.
        let stream = {
            let _guard = handle().enter();
            plan.execute(partition, task_ctx).map_err(|e| {
                ScanError::new(DfStatus::Execution, e.to_string())
            })?
        };
        Ok(StreamingReader { schema, stream })
    }

    /// Open one reader over the whole plan (all partitions coalesced).
    pub fn execute_all(&self) -> ScanResult<StreamingReader> {
        let plan = Arc::clone(&self.plan);
        let task_ctx = Arc::clone(&self.task_ctx);
        let schema: SchemaRef = plan.schema();
        let stream = {
            let _guard = handle().enter();
            execute_stream(plan, task_ctx)
                .map_err(|e| ScanError::new(DfStatus::Execution, e.to_string()))?
        };
        Ok(StreamingReader { schema, stream })
    }
}
