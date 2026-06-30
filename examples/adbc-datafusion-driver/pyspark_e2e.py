#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""End-to-end PySpark test of the adbc-datafusion connector + example driver.

PySpark drives the same JVM Spark, so the connector is used exactly as from
Scala/Java: spark.read.format("adbc-datafusion"). No Python-side connector code.

Prereqs:
  - arrow-adbc `main` (0.24.0-SNAPSHOT) built into ~/.m2 (Substrait + partitions),
  - the connector jar packaged (mvn -pl spark -Padbc-snapshot package),
  - its runtime deps copied to a dir (mvn dependency:copy-dependencies),
  - arrow-c-data (matching Spark's bundled Arrow, e.g. 18.1.0) added to that dir
    -- vanilla Spark bundles arrow-vector/arrow-memory but NOT arrow-c-data,
  - the example cdylib built (cargo build --release).

Configure via env vars (defaults assume this repo's build layout):
  CONNECTOR_JAR   path to datafusion-spark-*.jar
  ADBC_JARS_DIR   dir of runtime dep jars (adbc-*, substrait core, datafusion-java, ...)
  ADBC_DRIVER_LIB path to libadbc_datafusion_example_driver.{dylib,so}
"""

import glob
import os
import sys

from pyspark.sql import SparkSession

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
LIBEXT = "dylib" if sys.platform == "darwin" else "so"

connector_jar = os.environ.get(
    "CONNECTOR_JAR",
    glob.glob(os.path.join(REPO, "spark/target/datafusion-spark-*.jar"))[0],
)
jars_dir = os.environ.get("ADBC_JARS_DIR", "/tmp/adbc-libs")
driver_lib = os.environ.get(
    "ADBC_DRIVER_LIB",
    os.path.join(REPO, f"rust-target/release/libadbc_datafusion_example_driver.{LIBEXT}"),
)

jars = [connector_jar] + glob.glob(os.path.join(jars_dir, "*.jar"))
assert os.path.exists(driver_lib), f"driver cdylib not found: {driver_lib}"

# Java 17 opens Spark/Arrow need (Spark 4.0 sets most; be explicit for the data path).
opens = " ".join(
    f"--add-opens=java.base/{p}=ALL-UNNAMED"
    for p in ("java.nio", "sun.nio.ch", "java.lang", "java.util")
)

spark = (
    SparkSession.builder.master("local[2]")
    .appName("adbc-datafusion-pyspark-e2e")
    .config("spark.jars", ",".join(jars))
    .config("spark.driver.extraJavaOptions", opens)
    .config("spark.executor.extraJavaOptions", opens)
    .getOrCreate()
)

try:
    df = (
        spark.read.format("adbc-datafusion")
        .option("driver", driver_lib)
        .option("entrypoint", "AdbcDatafusionExampleInit")
        .option("table", "example")
        .load()
    )

    print("=== schema ===")
    df.printSchema()

    num_partitions = df.rdd.getNumPartitions()
    print("numPartitions:", num_partitions)

    ids = sorted(r["id"] for r in df.collect())
    names = sorted(r["name"] for r in df.select("name").collect())
    filtered = sorted(r["id"] for r in df.filter("id > 1").collect())
    print("full scan ids:", ids)
    print("projection names:", names)
    print("filter id>1:", filtered)

    assert ids == [1, 2, 3], ids
    assert names == ["alice", "bob", "carol"], names
    assert filtered == [2, 3], filtered
    assert num_partitions >= 2, f"expected multi-partition, got {num_partitions}"

    print("\nPYSPARK E2E OK (multi-partition + projection + filter pushdown)")
finally:
    spark.stop()
