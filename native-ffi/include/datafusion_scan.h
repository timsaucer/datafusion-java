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

// Plain-C scan ABI over the Arrow C Data / C Stream interface.
//
// The only "rich" types crossing this boundary are the standard Arrow C
// structs `ArrowSchema` and `ArrowArrayStream` (from Arrow's abi.h), which any
// Arrow implementation can produce/consume. Everything else is C primitives
// and borrowed (ptr, len) views. No JVM/JNI types appear here, by design.

#ifndef DATAFUSION_SCAN_H
#define DATAFUSION_SCAN_H

#include <stddef.h>
#include <stdint.h>

#include "arrow/c/abi.h"  // struct ArrowSchema, struct ArrowArrayStream

#ifdef __cplusplus
extern "C" {
#endif

// --- Status codes ----------------------------------------------------------
// 0 on success; nonzero classifies the failure. On error the call also writes
// a malloc'd, NUL-terminated message to *out_err (free with df_error_free).
typedef enum {
  DF_OK = 0,
  DF_INVALID_ARGUMENT = 1,
  DF_UNKNOWN_PROVIDER = 2,
  DF_PROVIDER_BUILD = 3,
  DF_PLANNING = 4,
  DF_EXECUTION = 5,
  DF_PANIC = 6,
  DF_INTERNAL = 7
} DfStatus;

// --- Borrowed input views (caller owns the memory) -------------------------
typedef struct {
  const uint8_t* ptr;  // UTF-8, not NUL-terminated; may be null if len == 0
  size_t len;
} DfStr;

typedef struct {
  const uint8_t* ptr;  // may be null if len == 0
  size_t len;
} DfBytes;

typedef struct {
  DfStr key;
  DfStr value;
} DfKeyValue;

// Opaque planned-scan handle.
typedef struct DfScanHandle DfScanHandle;

// --- Lifecycle / versioning ------------------------------------------------

// ABI major version; compare before any other call.
uint64_t df_scan_abi_version(void);

// Free a message previously written to an out_err argument (null-safe).
void df_error_free(char* err);

// --- Scan API --------------------------------------------------------------

// Probe a provider's output schema into the caller-allocated out_schema.
int32_t df_scan_schema(DfStr provider, DfBytes options, DfBytes partition,
                       struct ArrowSchema* out_schema, char** out_err);

// Plan a scan. On success writes an owned handle to *out_handle (release with
// df_scan_close). projection is an array of column-name DfStr (empty = all);
// filters is an array of serialized datafusion.LogicalExprNode DfBytes;
// target_partitions / batch_size <= 0 keep DataFusion defaults.
int32_t df_scan_create(DfStr provider, DfBytes options, DfBytes partition,
                       int32_t target_partitions, int32_t batch_size,
                       const DfKeyValue* config_overrides, size_t config_overrides_len,
                       const DfStr* projection, size_t projection_len,
                       const DfBytes* filters, size_t filters_len,
                       DfScanHandle** out_handle, char** out_err);

// Output partition count of the planned scan.
int32_t df_scan_partition_count(const DfScanHandle* handle, int32_t* out_count,
                                char** out_err);

// Execute one partition into the caller-allocated Arrow C Stream.
int32_t df_scan_execute_partition(const DfScanHandle* handle, int32_t partition,
                                  struct ArrowArrayStream* out_stream, char** out_err);

// Execute the whole plan as a single coalesced Arrow C Stream.
int32_t df_scan_execute(const DfScanHandle* handle,
                        struct ArrowArrayStream* out_stream, char** out_err);

// Drop a planned scan (null-safe). Must not race an in-flight execute on the
// same handle.
void df_scan_close(DfScanHandle* handle);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // DATAFUSION_SCAN_H
