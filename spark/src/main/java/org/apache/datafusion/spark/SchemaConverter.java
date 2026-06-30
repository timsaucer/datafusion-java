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

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Converts an Arrow schema (produced by the ADBC scan) into a Spark {@link StructType}.
 *
 * <p>Done directly rather than through Spark's {@code ArrowUtils} so the connector depends only on
 * our Arrow version, never Spark's bundled one. Covers the primitive types the columnar reader
 * produces; unsupported types fail fast.
 */
final class SchemaConverter {

  private SchemaConverter() {}

  static StructType toSparkSchema(Schema arrowSchema) {
    StructType struct = new StructType();
    for (Field field : arrowSchema.getFields()) {
      struct = struct.add(field.getName(), toSparkType(field), field.isNullable());
    }
    return struct;
  }

  static DataType toSparkType(Field field) {
    ArrowType type = field.getType();
    if (type instanceof ArrowType.Int i) {
      if (!i.getIsSigned()) {
        throw unsupported(field);
      }
      return switch (i.getBitWidth()) {
        case 8 -> DataTypes.ByteType;
        case 16 -> DataTypes.ShortType;
        case 32 -> DataTypes.IntegerType;
        case 64 -> DataTypes.LongType;
        default -> throw unsupported(field);
      };
    }
    if (type instanceof ArrowType.FloatingPoint fp) {
      return fp.getPrecision() == FloatingPointPrecision.DOUBLE
          ? DataTypes.DoubleType
          : DataTypes.FloatType;
    }
    if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) {
      return DataTypes.StringType;
    }
    if (type instanceof ArrowType.Bool) {
      return DataTypes.BooleanType;
    }
    throw unsupported(field);
  }

  private static IllegalArgumentException unsupported(Field field) {
    return new IllegalArgumentException(
        "unsupported Arrow type for column '" + field.getName() + "': " + field.getType());
  }
}
