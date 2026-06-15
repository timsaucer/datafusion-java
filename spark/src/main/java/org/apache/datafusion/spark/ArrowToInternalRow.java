/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datafusion.spark;

import java.util.List;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Reads one row out of an Arrow {@link VectorSchemaRoot} into a Spark {@link InternalRow}.
 *
 * <p>Row-based on purpose: it touches only our Arrow version, so the connector never shares Arrow
 * with Spark's bundled copy. Columnar (Spark {@code ArrowColumnVector}) would be faster but couples
 * the two Arrow versions. Handles the primitive types {@link SchemaConverter} maps.
 */
final class ArrowToInternalRow {

  private ArrowToInternalRow() {}

  static InternalRow convert(VectorSchemaRoot root, int row) {
    List<FieldVector> vectors = root.getFieldVectors();
    Object[] values = new Object[vectors.size()];
    for (int col = 0; col < vectors.size(); col++) {
      values[col] = value(vectors.get(col), row);
    }
    return new GenericInternalRow(values);
  }

  private static Object value(FieldVector vector, int row) {
    if (vector.isNull(row)) {
      return null;
    }
    if (vector instanceof BigIntVector v) {
      return v.get(row);
    }
    if (vector instanceof IntVector v) {
      return v.get(row);
    }
    if (vector instanceof SmallIntVector v) {
      return v.get(row);
    }
    if (vector instanceof TinyIntVector v) {
      return v.get(row);
    }
    if (vector instanceof Float8Vector v) {
      return v.get(row);
    }
    if (vector instanceof Float4Vector v) {
      return v.get(row);
    }
    if (vector instanceof BitVector v) {
      return v.get(row) != 0;
    }
    if (vector instanceof VarCharVector v) {
      return UTF8String.fromBytes(v.get(row));
    }
    throw new IllegalArgumentException(
        "unsupported Arrow vector for column '"
            + vector.getField().getName()
            + "': "
            + vector.getClass().getSimpleName());
  }
}
