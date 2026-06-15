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

fn main() {
    // scan_config.proto embeds the per-format read-option messages, which in
    // turn import file_compression_type; every transitively-referenced file
    // must be compiled so the generated `ScanConfig` has its field types.
    const PROTOS: &[&str] = &[
        "../proto/scan_config.proto",
        "../proto/scan_request.proto",
        "../proto/file_compression_type.proto",
        "../proto/csv_read_options.proto",
        "../proto/json_read_options.proto",
        "../proto/parquet_read_options.proto",
        "../proto/avro_read_options.proto",
        "../proto/arrow_read_options.proto",
    ];
    for p in PROTOS {
        println!("cargo:rerun-if-changed={p}");
    }
    // Honor a caller-provided PROTOC (e.g. a system install) and otherwise fall
    // back to the vendored binary, matching `native/build.rs`.
    if std::env::var_os("PROTOC").is_none() {
        let protoc =
            protoc_bin_vendored::protoc_bin_path().expect("vendored protoc not available");
        std::env::set_var("PROTOC", protoc);
    }
    prost_build::compile_protos(PROTOS, &["../proto"]).expect("failed to compile protos");
}
