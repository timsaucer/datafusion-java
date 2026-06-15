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

//! A real file-backed provider builder, registered under `datafusion.listing`.
//!
//! Decodes the [`ScanConfig`](crate::proto::ScanConfig) blob into a DataFusion
//! [`ListingTable`] over one or more paths read with a single file format.
//! Demonstrates a builder that needs the session context: when no explicit
//! schema is supplied it infers one from the data (and the context's object
//! store registry resolves the paths).
//!
//! Object stores for remote URIs (s3://, gs://, ...) must be registered on the
//! context by the embedding cdylib before a scan runs; the default context
//! resolves local paths out of the box.

use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::catalog::TableProvider;
use datafusion::datasource::file_format::arrow::ArrowFormat;
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use prost::Message;

use crate::error::{DfStatus, ScanError, ScanResult};
use crate::proto::{listing_source, scan_config, FileCompressionType as ProtoCompression};
use crate::proto::{ListingSource, ScanConfig};
use crate::registry::register_provider;
use crate::runtime::handle;

/// Registered builder name for the listing provider.
pub const NAME: &str = "datafusion.listing";

/// Register the listing provider. Call once at startup.
pub fn register() {
    register_provider(NAME, build);
}

fn build(
    ctx: &SessionContext,
    options: &[u8],
    _partition: &[u8],
) -> ScanResult<Arc<dyn TableProvider>> {
    let config = ScanConfig::decode(options).map_err(|e| {
        ScanError::new(
            DfStatus::ProviderBuild,
            format!("failed to decode ScanConfig: {e}"),
        )
    })?;

    let listing = match config.source {
        Some(scan_config::Source::Listing(l)) => l,
        Some(scan_config::Source::Custom(_)) => {
            return Err(ScanError::new(
                DfStatus::ProviderBuild,
                "datafusion.listing requires a listing source, got custom bytes",
            ))
        }
        None => {
            return Err(ScanError::new(
                DfStatus::ProviderBuild,
                "datafusion.listing requires a listing source, none set",
            ))
        }
    };

    if listing.paths.is_empty() {
        return Err(ScanError::new(
            DfStatus::ProviderBuild,
            "listing source has no paths",
        ));
    }

    let table_paths = listing
        .paths
        .iter()
        .map(|p| {
            ListingTableUrl::parse(p).map_err(|e| {
                ScanError::new(DfStatus::ProviderBuild, format!("invalid path {p:?}: {e}"))
            })
        })
        .collect::<ScanResult<Vec<_>>>()?;

    let listing_options = listing_options(&listing)?;

    let mut table_config =
        ListingTableConfig::new_with_multi_paths(table_paths).with_listing_options(listing_options);

    table_config = match &listing.schema_ipc {
        Some(bytes) => table_config.with_schema(schema_from_ipc(bytes)?),
        // No explicit schema: infer from the data, using the context's state
        // (and thus its object store registry) to read it.
        None => handle()
            .block_on(table_config.infer_schema(&ctx.state()))
            .map_err(|e| {
                ScanError::new(
                    DfStatus::ProviderBuild,
                    format!("failed to infer listing schema: {e}"),
                )
            })?,
    };

    let table = ListingTable::try_new(table_config)
        .map_err(|e| ScanError::new(DfStatus::ProviderBuild, e.to_string()))?;
    Ok(Arc::new(table))
}

/// Map the proto format oneof to a DataFusion [`ListingOptions`]. Covers the
/// option fields the read-option messages expose today; unset fields keep the
/// format's defaults.
fn listing_options(listing: &ListingSource) -> ScanResult<ListingOptions> {
    use listing_source::Format;

    let (format, default_ext): (Arc<dyn FileFormat>, &str) = match &listing.format {
        Some(Format::Csv(c)) => {
            let mut fmt = CsvFormat::default()
                .with_has_header(c.has_header)
                .with_delimiter(byte(c.delimiter, b',')?)
                .with_quote(byte(c.quote, b'"')?)
                .with_newlines_in_values(c.newlines_in_values.unwrap_or(false))
                .with_file_compression_type(compression(c.file_compression_type));
            if let Some(t) = c.terminator {
                fmt = fmt.with_terminator(Some(byte(t, b'\n')?));
            }
            if let Some(e) = c.escape {
                fmt = fmt.with_escape(Some(byte(e, b'\\')?));
            }
            if let Some(cm) = c.comment {
                fmt = fmt.with_comment(Some(byte(cm, b'#')?));
            }
            (Arc::new(fmt), extension(&c.file_extension, ".csv"))
        }
        Some(Format::Json(j)) => {
            let fmt = JsonFormat::default()
                .with_file_compression_type(compression(j.file_compression_type));
            (Arc::new(fmt), extension(&j.file_extension, ".json"))
        }
        Some(Format::Parquet(p)) => {
            // Parquet read tuning (pruning / metadata hints) is applied through
            // session config at scan time, not on the format here.
            (
                Arc::new(ParquetFormat::default()),
                extension(&p.file_extension, ".parquet"),
            )
        }
        Some(Format::Avro(a)) => (Arc::new(AvroFormat), extension(&a.file_extension, ".avro")),
        Some(Format::Arrow(a)) => (
            Arc::new(ArrowFormat),
            extension(&a.file_extension, ".arrow"),
        ),
        None => {
            return Err(ScanError::new(
                DfStatus::ProviderBuild,
                "listing source has no file format",
            ))
        }
    };

    Ok(ListingOptions::new(format).with_file_extension(default_ext.to_string()))
}

/// A single byte sent over the wire as a `uint32`. Falls back to `default` when
/// the field is unset (0), and rejects values that do not fit in a byte.
fn byte(value: u32, default: u8) -> ScanResult<u8> {
    if value == 0 {
        return Ok(default);
    }
    u8::try_from(value)
        .map_err(|_| ScanError::invalid_argument(format!("byte option {value} exceeds 255")))
}

fn extension<'a>(configured: &'a str, default: &'a str) -> &'a str {
    if configured.is_empty() {
        default
    } else {
        configured
    }
}

fn compression(value: i32) -> FileCompressionType {
    match ProtoCompression::try_from(value) {
        Ok(ProtoCompression::Gzip) => FileCompressionType::GZIP,
        Ok(ProtoCompression::Bzip2) => FileCompressionType::BZIP2,
        Ok(ProtoCompression::Xz) => FileCompressionType::XZ,
        Ok(ProtoCompression::Zstd) => FileCompressionType::ZSTD,
        // Unspecified / uncompressed / unknown -> uncompressed.
        _ => FileCompressionType::UNCOMPRESSED,
    }
}

/// Read a `SchemaRef` from Arrow IPC stream bytes (a schema message, optionally
/// followed by zero batches -- the shape `StreamWriter::finish` produces).
fn schema_from_ipc(bytes: &[u8]) -> ScanResult<SchemaRef> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(|e| {
        ScanError::new(
            DfStatus::ProviderBuild,
            format!("failed to read schema_ipc: {e}"),
        )
    })?;
    let schema: Schema = reader.schema().as_ref().clone();
    Ok(Arc::new(schema))
}
