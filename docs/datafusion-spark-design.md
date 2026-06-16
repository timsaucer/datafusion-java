# DataFusion-backed Spark DataSource: design

## Goal

Let Spark read from a DataFusion `TableProvider` as a native `DataSourceV2`,
with the native boundary placed at the **Arrow C Data / C Stream interface and
plain C types** — not at handwritten JNI per operation.

## Origin

On [PR #104](https://github.com/apache/datafusion-java/pull/104), Dewey
Dunnington (@paleolimbot) reviewed an earlier stack (PR #103) whose cdylib
exported JNI entry points directly, and argued for a cleaner shape:

> build a cdylib that exports entrypoints that just use the Arrow C Data/Stream
> interface and C types. That also has broader applicability to non-Java (i.e.,
> can live in datafusion proper and get eyes/reviews from a wider audience).

This design follows that: the reusable artifact is a **plain-C scan ABI** over
Arrow C types; JNI is a thin, separable adapter; the same ABI is callable from
Python/Go/Rust/FFM. "Approach A" — the providers we ship are compiled into the
cdylib and selected by name, rather than imported over `datafusion-ffi`.

## Principle: two planes, both zero-copy

| Plane | Carries | Crosses via |
| --- | --- | --- |
| **Data** | Arrow record batches | Arrow C Stream (`FFI_ArrowArrayStream`) → arrow-java import → Spark `ArrowColumnVector` |
| **Control** | provider name, config, pushdown, partition index | plain-C calls passing `(ptr, len)` and `long` addresses |

No Arrow data is ever marshaled through JNI. Batches flow through the Arrow C
Data interface, which arrow-java and arrow-rs already speak; the JVM gets real
Arrow vectors and hands them to Spark with no per-cell copy.

## Architecture

```
 spark.read.format("datafusion").option("path", ...).load()
   │
   ▼  datafusion-spark (Maven module, Java, Spark 4.0)
   │  TableProvider → Table → ScanBuilder (projection / filter / limit pushdown)
   │  → Scan/Batch → InputPartition[]  (serializable: config + request bytes + index)
   │  → PartitionReaderFactory → ColumnarPartitionReader
   │
   ▼  core: org.apache.datafusion.scan.DatafusionScan   (JVM scan API)
   │  + NativeScan (6 JNI methods)  ──loads──►  libdatafusion_scan_jni
   │
   ▼  native-jni: datafusion-scan-jni (cdylib)         ← thin JVM adapter
   │  Java_…_NativeScan_*  → calls the scan core; writes FFI_ArrowArrayStream
   │                          into the address arrow-java allocated
   │
   ▼  native-ffi: datafusion-scan-ffi (cdylib + rlib)  ← the reusable plain-C ABI
   │  df_scan_* (extern "C")  → scan core → registered provider builder
   │  data plane: FFI_ArrowArrayStream  (arrow-rs)
   │
   ▼  DataFusion: TableProvider (e.g. ListingTable) reads the source
```

Non-Java consumers (Python/Go/Rust/FFM) bind `df_scan_*` directly and skip the
JNI and Spark layers entirely.

## Components

| Path | Crate / module | Role |
| --- | --- | --- |
| `native-ffi/` | `datafusion-scan-ffi` (cdylib + rlib) | The plain-C scan ABI; scan core; provider registry; demo + `datafusion.listing` providers |
| `native-jni/` | `datafusion-scan-jni` (cdylib) | Thin JNI shim over the scan core |
| `core/.../scan/` | part of `datafusion-java` | `NativeScan` (native decls), `ScanNativeLoader`, `DatafusionScan` (JVM API) |
| `spark/` | `datafusion-spark` (Java) | The Spark `DataSourceV2` connector |
| `proto/` | shared | `scan_config.proto`, `scan_request.proto` |

## The plain-C ABI (`native-ffi/include/datafusion_scan.h`)

```c
uint64_t df_scan_abi_version(void);
void     df_error_free(char* err);

int32_t df_scan_schema(DfStr provider, DfBytes options, DfBytes partition,
                       struct ArrowSchema* out_schema, char** out_err);
int32_t df_scan_create(DfStr provider, DfBytes options, DfBytes partition,
                       int32_t target_partitions, int32_t batch_size, int64_t limit,
                       const DfKeyValue* config_overrides, size_t config_overrides_len,
                       const DfStr* projection, size_t projection_len,
                       const DfBytes* filters, size_t filters_len,
                       DfScanHandle** out_handle, char** out_err);
int32_t df_scan_partition_count(const DfScanHandle*, int32_t* out_count, char** out_err);
int32_t df_scan_execute_partition(const DfScanHandle*, int32_t partition,
                                  struct ArrowArrayStream* out_stream, char** out_err);
int32_t df_scan_execute(const DfScanHandle*, struct ArrowArrayStream* out_stream, char** out_err);
void    df_scan_close(DfScanHandle*);
```

Conventions: every fallible call returns `0` / nonzero `DfStatus`, writing a
malloc'd message to `*out_err` (freed by `df_error_free`). The only "rich" types
crossing are the standard Arrow C structs `ArrowSchema` / `ArrowArrayStream`.
Each call is wrapped in `catch_unwind` so a Rust panic becomes a status code,
never an unwind across the C boundary.

Providers are registered by name (`register_provider`) and select via the
`provider` argument; the `options`/`partition` blobs are opaque to the ABI and
decoded by the registered builder.

## Wire formats (`proto/`)

- **`ScanConfig`** — the `options` blob: `provider` name + a `source` oneof
  (`ListingSource` reusing the per-format read-option messages, or a `custom`
  bytes escape hatch). `ScanPartition` is the per-partition `partition` blob.
- **`ScanRequest`** — the engine's pushdown: `projection` (column names),
  `filters` (each a serialized `datafusion.LogicalExprNode`), `limit`,
  `target_partitions`, `batch_size`, `config_overrides`.

`ScanRequest` is decoded by the JNI shim and exploded into `df_scan_create`'s
typed C arguments, rather than passed as one blob — keeping the C ABI typed and
FFM-friendly. Filters reuse DataFusion's own `LogicalExprNode` proto, so the
Java side generates builders and the Rust side decodes with the stock codec from
the same `.proto` — and the encoding is shared with any future Comet path.

## JNI shim (`native-jni` + `core/.../scan`)

Six `Java_…NativeScan_*` methods: `providerSchema`, `createScan`,
`partitionCount`, `executeStreamPartition`, `executeStream`, `closeScan`. Each
marshals a `String` + `byte[]`s and `long` addresses; the data plane writes an
`FFI_ArrowArrayStream` into the arrow-java-allocated struct. `DatafusionScan`
wraps these and returns an `ArrowReader` via `Data.importArrayStream`, mirroring
`core`'s existing `DataFrame#collect`.

## Arrow version strategy (the key integration decision)

`ArrowColumnVector` is zero-copy only if the vectors we hand it are the **same
arrow-java classes** Spark loaded — i.e. one Arrow in the executor JVM. So the
connector treats arrow-java as **`provided`**: the cluster supplies it, our
stream import and Spark's `ArrowColumnVector` share it, and columnar works with
whatever Arrow the deployment ships (within an API-compatible window of the
compile baseline, currently Spark 4.0's Arrow 18.1).

Consequences:

- **`datafusion-java` (core) stays on Arrow 19** for standalone use; only its
  Arrow transitive is excluded from the Spark module. No main downgrade.
- **The Rust side is unaffected.** The Arrow C Data interface is a stable spec,
  independent of Arrow library version: `arrow-rs 58` producing an
  `FFI_ArrowArrayStream` imports into arrow-java 18 or 19 alike. Verified by the
  JVM round-trip test.

## Spark DataSourceV2 mapping

| Spark interface | Our class | Behaviour |
| --- | --- | --- |
| `TableProvider`, `DataSourceRegister` | `DatafusionTableProvider` | `"datafusion"` short name; `inferSchema` probes via `df_scan_schema` |
| `Table`, `SupportsRead` | `DatafusionTable` | `BATCH_READ` capability |
| `ScanBuilder` + `SupportsPushDown{RequiredColumns,Filters,Limit}` | `DatafusionScanBuilder` | encodes projection / filters / limit into `ScanRequest` |
| `Scan`, `Batch` | `DatafusionScanImpl` | plans once on the driver for partition count |
| `InputPartition` | `DatafusionInputPartition` | **serializable**: carries config + request bytes + index, never a native handle |
| `PartitionReaderFactory` | `DatafusionPartitionReaderFactory` | columnar reads |
| `PartitionReader<ColumnarBatch>` | `DatafusionColumnarPartitionReader` | wraps imported Arrow vectors in `ArrowColumnVector`, zero-copy |

Helpers: `OptionsCodec` (Spark options → `ScanConfig`), `SchemaConverter` (Arrow
schema → Spark `StructType`, using only our Arrow types), `SparkFilters` (Spark
`Filter`s → `LogicalExprNode`: comparisons, `And`/`Or`/`Not`, `IsNull`/
`IsNotNull` over primitive literals; anything else falls back to Spark).

**Partition serialization constraint:** a native handle is meaningless in
another executor process, so partitions carry only bytes + an index, and each
executor rebuilds the provider and runs its own partition. A limited plan
coalesces to one partition, so `pushLimit` can report the bound as fully
handled.

## Testing

| Level | Where | Proves |
| --- | --- | --- |
| Rust ABI round-trip | `native-ffi/tests/roundtrip.rs` | `df_scan_*` + import the stream back via the Arrow C Stream interface; partition count; limit; error/status |
| Rust proto | `native-ffi/tests/proto.rs` | `ScanConfig`/`ScanRequest` encode/decode incl. embedded read-options |
| Rust listing | `native-ffi/tests/listing.rs` | real `ListingTable` over a CSV, schema inference, full scan |
| JVM scan | `core/.../scan/DatafusionScanTest` | end-to-end Java → JNI → Arrow C Stream; schema, scan, projection, filter, limit (closes the arrow-rs 58 ↔ arrow-java 19 ABI question) |
| Spark unit | `spark/.../DatafusionScanBuilderTest` | decodes the built `ScanRequest` to prove pushdown is actually encoded (isolated from Spark's own handling) |
| Spark E2E | `spark/.../DatafusionSourceTest` | local `SparkSession` over `format("datafusion")`: schema, full scan, projection, filter, limit on Spark 4.0 columnar |

## Decisions log

- **Approach A over `datafusion-ffi` import.** `datafusion-ffi` already exposes
  the whole `TableProvider`, but over stabby vtables + an async, poll-based
  `FFI_RecordBatchStream` — not Java-consumable and not flat C. Compiling
  providers in and exporting flat C is simpler and is exactly the shape Dewey
  asked for. The async surface would only be needed to load *third-party*
  provider cdylibs (a future option B).
- **Plain C + thin JNI, not JNI-in-the-cdylib.** Keeps the reusable artifact
  language-neutral and upstreamable; quarantines the JVM into a ~6-method shim.
- **Row-based → columnar.** Shipped row-based first to decouple from Spark's
  Arrow, then moved to columnar once the `provided`-Arrow strategy removed the
  version clash. Columnar is zero-copy; row-based is gone.
- **Spark 4.0 / Arrow 18.1 baseline, Java.** Java matches the rest of the stack;
  Spark 4.0's Arrow (18.1) is close to ours and Java-17 native.

## Status and gaps

Built and green end to end: the plain-C ABI, the JNI shim, and a columnar Spark
4.0 connector with projection / filter / limit pushdown.

Not yet done:

- **Multi-partition coverage.** The executor-rebuild path is wired but exercised
  only at one partition (single CSV); a directory/Parquet test would cover N>1.
- **Native library packaging.** The shim loads from `java.library.path`;
  classpath bundling per OS/arch (as `core` does for `datafusion_jni`) is left
  to release packaging.
- **Format breadth.** CSV options are fully mapped; Parquet/Avro/Arrow use
  defaults.
- **External provider cdylibs (option B).** Loading third-party providers over
  `datafusion-ffi`'s `ForeignTableProvider` is not implemented.

## Alternative / companion front-end: ADBC

A reviewer suggested exposing arbitrary DataFusion `TableProvider`s over
[ADBC](https://arrow.apache.org/adbc/) (Arrow Database Connectivity) instead of —
or alongside — this scan ABI. The two are not mutually exclusive: they are two
front-ends over the same core, serving different consumers.

### What this PR's work reuses

The PR already cleaves at the right seam. Three layers, and the valuable two are
front-end-agnostic:

| Layer | ADBC reuse |
| --- | --- |
| Exec core (`scan.rs`, `reader.rs`, `runtime.rs`) — build provider → register on `SessionContext` → plan → `ExecutionPlan` → partition stream → `FFI_ArrowArrayStream` | **Direct reuse.** Already JVM-free and C-free. |
| Provider registry (`registry.rs`) — register `TableProvider` by name, build on demand | **Direct reuse.** This *is* the "arbitrary providers" mechanism. |
| `native-common` (errors, tokio handle); panic→status `catch_unwind` pattern | Reuse concept; ADBC has its own error struct. |
| `df_scan_*` flat C ABI, proto pushdown (`ScanRequest` / `SparkFilters` / `LogicalExprNode`), JNI shim, `core/scan/*`, `spark/*` | **Not reused.** Scan-, JVM-, and Spark-specific. |

`reader.rs`'s `StreamingReader` (DataFusion `SendableRecordBatchStream` →
`ArrowArrayStream`) is exactly what ADBC's `AdbcStatementExecuteQuery` returns:
the data plane is identical, and the cross-implementation Arrow C Stream question
this PR already answered carries over unchanged.

### What ADBC adds, and what it drops

ADBC mandates a fixed, large C surface — `AdbcDatabase` / `AdbcConnection` /
`AdbcStatement` lifecycle, option getters/setters, metadata calls, an
`AdbcDriverInit` entry point. You do **not** hand-write that vtable: the official
`adbc_core` Rust crate supplies `Database` / `Connection` / `Statement` traits
plus an `export_driver!` macro that generates the C ABI. So the FFI layer becomes
trait glue, not a second hand-written boundary.

New work:

- `adbc_core` dependency + three trait impls. `Database` holds config + registered
  providers; `Connection` wraps a `SessionContext`; `Statement` holds SQL + bound
  params and, on execute, runs `ctx.sql(q)` → physical plan → the existing
  `StreamingReader`.
- Catalog metadata methods (`GetObjects` / `GetTableSchema` / `GetTableTypes` /
  `GetInfo`) → DataFusion `CatalogProvider` / `SchemaProvider` introspection.
- ADBC error / status mapping in place of `DfStatus`.
- Optional: parameter binding / prepared statements; `ExecutePartitions` (maps
  cleanly onto the existing plan-partition logic); ingest/write (likely out of
  scope).
- Driver packaging (a manifest so `adbc_driver_manager` can load the library).

Dropped relative to the Spark path: the protobuf pushdown machinery
(`ScanRequest`, `SparkFilters`, `LogicalExprNode` encoding) is unneeded — ADBC
clients send SQL and DataFusion's optimizer does pushdown internally — as are the
JNI shim, `core/scan`, and the Spark module.

### Suggested layout for both

```
native-common/        errors, tokio runtime           [shared]
native-exec-core/     provider registry + plan/exec   [shared]  ← lift scan.rs/reader.rs/registry.rs here
  ├─ native-ffi/      df_scan_* flat C (+ JNI/Spark)   [exists]
  └─ native-adbc/     adbc_core trait impls            [new]
```

One refactor on the existing side: lift `scan.rs` / `reader.rs` / `registry.rs`
out of `native-ffi` into a shared `native-exec-core` crate that both front-ends
depend on; `native-ffi` keeps only `abi.rs` + proto. Low churn — those modules
are already free of C/JVM concerns by design.

### Why keep both rather than collapse to one

Different consumers. `df_scan_*` is a bespoke, scan-only ABI with **explicit**
pushdown: every consumer hand-binds it, but it can carry Spark's pre-resolved
predicates without a SQL round-trip. ADBC is a SQL-oriented **standard** ABI:
bigger mandated surface, but the whole client ecosystem (Python
`adbc_driver_manager`, R, Go, the JDBC↔ADBC bridge) comes for free.

They are not redundant, because Spark's pre-resolved pushdown does not always
re-serialize to a SQL string:

- **Lossy but rescuable** (within current filter scope): float/double literals
  (decimal-text render loses exact IEEE bits), `NaN`/`±Inf` (no SQL literal),
  decimal precision/scale, binary/non-UTF8 literals, null-safe equality
  (`<=>` → `IS NOT DISTINCT FROM`), identifier quoting/case. ADBC parameter
  binding (`WHERE col = ?` with a typed bound value) closes most of the literal
  cases.
- **Structurally impossible**: pushdown whose value is not known at
  statement-prepare time — dynamic partition pruning, runtime/bloom filters from
  joins — cannot be a static SQL string, and binding does not help because the
  value arrives mid-execution. This PR pushes none of these yet, but it is the
  reason a typed-`Expr` scan ABI is not merely a convenience over SQL: it is the
  only path that can carry runtime filters at all.

So the recommendation is a shared `native-exec-core` with two thin front-ends:
ADBC for SQL clients across the Arrow ecosystem, the flat-C scan ABI for
embedders (Spark today) that push pre-resolved or runtime predicates.
