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

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Converts an Arrow schema (produced by the ADBC scan) into a Spark {@link StructType}, and plans
 * the source-side casts that make the scan emit Spark-native Arrow.
 *
 * <p>Done directly rather than through Spark's {@code ArrowUtils} so the connector depends only on
 * our Arrow version, never Spark's bundled one.
 *
 * <p>Spark's vectorized {@code ArrowColumnVector} reads a fixed set of Arrow layouts: signed ints,
 * 32/64-bit floats, microsecond timestamps, string/binary, decimal, date, and nested
 * list/struct/map of those. Two categories of source type need handling:
 *
 * <ul>
 *   <li><b>Directly representable</b> -- the layout already matches; only the type mapping was
 *       missing (binary, nested list/struct/map, date, decimal, µs timestamp, null). These pass
 *       through untouched.
 *   <li><b>Cast required</b> -- the layout differs from what {@code ArrowColumnVector} expects, so
 *       the scan must cast at the source (unsigned ints, Float16, non-µs timestamps, time). We map
 *       these to the Spark type they will be cast <em>to</em>, and {@link #castTargetString} names
 *       the Arrow target so {@link SqlQuery} can wrap the column in {@code arrow_cast}.
 * </ul>
 *
 * <p>The cast is pushed into the scan (see {@link SqlQuery}), so this converter and the reader only
 * ever agree on Spark-native types: the reported Spark type of a column always equals what {@code
 * ArrowColumnVector} produces from the (possibly cast) Arrow output.
 */
final class SchemaConverter {

  /**
   * Metadata flag set on a top-level Spark field whose source column needs a cast. Read by {@link
   * AdbcScanBuilder} to keep filter pushdown off these columns: a pushed predicate runs against the
   * pre-cast source column, so its literal would be in the wrong (source, not Spark) domain.
   */
  static final String CAST_METADATA_KEY = "org.apache.datafusion.spark.adbc.cast";

  private SchemaConverter() {}

  /** A projected output column: its name and, when a cast is required, the Arrow target type. */
  record ProjectionColumn(String name, String castType) {}

  static StructType toSparkSchema(Schema arrowSchema) {
    StructType struct = new StructType();
    for (Field field : arrowSchema.getFields()) {
      Metadata metadata =
          needsCast(field)
              ? new MetadataBuilder().putBoolean(CAST_METADATA_KEY, true).build()
              : Metadata.empty();
      struct = struct.add(field.getName(), toSparkType(field), field.isNullable(), metadata);
    }
    return struct;
  }

  /**
   * Plan the projected columns for the pushed scan: the kept columns in output order, each with the
   * Arrow cast target (or {@code null} to pass through).
   *
   * @param schema the full source Arrow schema
   * @param projection kept column names in output order, or {@code null} for all columns
   */
  static List<ProjectionColumn> projectionColumns(Schema schema, List<String> projection) {
    List<Field> fields = new ArrayList<>();
    if (projection == null) {
      fields.addAll(schema.getFields());
    } else {
      for (String name : projection) {
        fields.add(schema.findField(name));
      }
    }
    List<ProjectionColumn> columns = new ArrayList<>(fields.size());
    for (Field field : fields) {
      columns.add(new ProjectionColumn(field.getName(), castTargetString(field)));
    }
    return columns;
  }

  /** The Arrow type string for {@code arrow_cast}, or {@code null} if the column needs no cast. */
  static String castTargetString(Field field) {
    return needsCast(field) ? renderArrowType(field) : null;
  }

  /**
   * A single Spark-readable column for a column-less scan (e.g. {@code count()}, whose projection
   * Catalyst prunes to empty). Such a scan only needs a row count, but the emitted stream must
   * still be Spark-native -- a bare {@code SELECT *} would return the raw, uncast schema and the
   * reader would fail on the first non-Spark-native column. Prefers a column that needs no cast
   * (cheapest); falls back to the first column with its cast applied when every column needs one.
   */
  static ProjectionColumn probeColumn(Schema schema) {
    for (Field field : schema.getFields()) {
      if (!needsCast(field)) {
        return new ProjectionColumn(field.getName(), null);
      }
    }
    Field first = schema.getFields().get(0);
    return new ProjectionColumn(first.getName(), castTargetString(first));
  }

  // --- Arrow type -> Spark type ---------------------------------------------

  static DataType toSparkType(Field field) {
    ArrowType type = field.getType();
    if (type instanceof ArrowType.Bool) {
      return DataTypes.BooleanType;
    }
    if (type instanceof ArrowType.Int i) {
      if (i.getIsSigned()) {
        return switch (i.getBitWidth()) {
          case 8 -> DataTypes.ByteType;
          case 16 -> DataTypes.ShortType;
          case 32 -> DataTypes.IntegerType;
          case 64 -> DataTypes.LongType;
          default -> throw unsupported(field);
        };
      }
      // Unsigned: widened to the next signed width it will be cast to (u64 has no lossless
      // signed 64-bit target, so it becomes Decimal(20,0)).
      return switch (i.getBitWidth()) {
        case 8 -> DataTypes.ShortType;
        case 16 -> DataTypes.IntegerType;
        case 32 -> DataTypes.LongType;
        case 64 -> DataTypes.createDecimalType(20, 0);
        default -> throw unsupported(field);
      };
    }
    if (type instanceof ArrowType.FloatingPoint fp) {
      // Float16 has no Spark type; it is widened to Float.
      return fp.getPrecision() == FloatingPointPrecision.DOUBLE
          ? DataTypes.DoubleType
          : DataTypes.FloatType;
    }
    if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) {
      return DataTypes.StringType;
    }
    if (type instanceof ArrowType.Binary
        || type instanceof ArrowType.LargeBinary
        || type instanceof ArrowType.FixedSizeBinary) {
      return DataTypes.BinaryType;
    }
    if (type instanceof ArrowType.Date) {
      return DataTypes.DateType;
    }
    if (type instanceof ArrowType.Timestamp ts) {
      // Unit is normalized to microseconds by the cast; the timezone decides NTZ vs zoned.
      return ts.getTimezone() == null ? DataTypes.TimestampNTZType : DataTypes.TimestampType;
    }
    if (type instanceof ArrowType.Time t) {
      // Spark has no time-of-day accessor; the value is cast to its raw integer of ticks.
      return t.getBitWidth() == 32 ? DataTypes.IntegerType : DataTypes.LongType;
    }
    if (type instanceof ArrowType.Decimal d) {
      return DataTypes.createDecimalType(d.getPrecision(), d.getScale());
    }
    if (type instanceof ArrowType.Null) {
      return DataTypes.NullType;
    }
    if (type instanceof ArrowType.Duration) {
      return DataTypes.createDayTimeIntervalType();
    }
    if (type instanceof ArrowType.Interval iv) {
      return switch (iv.getUnit()) {
        case YEAR_MONTH -> DataTypes.createYearMonthIntervalType();
        case DAY_TIME -> DataTypes.createDayTimeIntervalType();
        default -> throw unsupported(field);
      };
    }
    if (type instanceof ArrowType.List
        || type instanceof ArrowType.LargeList
        || type instanceof ArrowType.FixedSizeList) {
      Field element = field.getChildren().get(0);
      return DataTypes.createArrayType(toSparkType(element), element.isNullable());
    }
    if (type instanceof ArrowType.Struct) {
      List<StructField> children = new ArrayList<>();
      for (Field child : field.getChildren()) {
        children.add(
            DataTypes.createStructField(child.getName(), toSparkType(child), child.isNullable()));
      }
      return DataTypes.createStructType(children);
    }
    if (type instanceof ArrowType.Map) {
      Field entries = field.getChildren().get(0);
      Field key = entries.getChildren().get(0);
      Field value = entries.getChildren().get(1);
      return DataTypes.createMapType(toSparkType(key), toSparkType(value), value.isNullable());
    }
    throw unsupported(field);
  }

  // --- Cast planning --------------------------------------------------------

  /**
   * Whether the column (recursively) has any layout that Spark's reader cannot consume directly.
   */
  static boolean needsCast(Field field) {
    ArrowType type = field.getType();
    if (type instanceof ArrowType.Int i && !i.getIsSigned()) {
      return true;
    }
    if (type instanceof ArrowType.FloatingPoint fp
        && fp.getPrecision() == FloatingPointPrecision.HALF) {
      return true;
    }
    if (type instanceof ArrowType.Timestamp ts && ts.getUnit() != TimeUnit.MICROSECOND) {
      return true;
    }
    if (type instanceof ArrowType.Time) {
      return true;
    }
    // Spark's ArrowColumnVector backs ArrayType only from a variable ListVector, never a
    // FixedSizeListVector, so a fixed-size list must always be cast to a variable list.
    if (type instanceof ArrowType.FixedSizeList) {
      return true;
    }
    for (Field child : field.getChildren()) {
      if (needsCast(child)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Render the widened Arrow type as an {@code arrow_cast} type string (the reversible {@code
   * arrow::datatypes::DataType} display form that DataFusion's {@code arrow_cast} parses). Cast
   * leaves become their widened target; everything else is rendered as-is so a nested cast carries
   * its unchanged siblings along.
   */
  private static String renderArrowType(Field field) {
    ArrowType type = field.getType();
    if (type instanceof ArrowType.Bool) {
      return "Boolean";
    }
    if (type instanceof ArrowType.Int i) {
      if (i.getIsSigned()) {
        return "Int" + i.getBitWidth();
      }
      return switch (i.getBitWidth()) {
        case 8 -> "Int16";
        case 16 -> "Int32";
        case 32 -> "Int64";
        case 64 -> "Decimal128(20, 0)";
        default -> throw unsupported(field);
      };
    }
    if (type instanceof ArrowType.FloatingPoint fp) {
      return fp.getPrecision() == FloatingPointPrecision.DOUBLE ? "Float64" : "Float32";
    }
    if (type instanceof ArrowType.Utf8) {
      return "Utf8";
    }
    if (type instanceof ArrowType.LargeUtf8) {
      return "LargeUtf8";
    }
    if (type instanceof ArrowType.Binary) {
      return "Binary";
    }
    if (type instanceof ArrowType.LargeBinary) {
      return "LargeBinary";
    }
    if (type instanceof ArrowType.FixedSizeBinary fb) {
      return "FixedSizeBinary(" + fb.getByteWidth() + ")";
    }
    if (type instanceof ArrowType.Date d) {
      return switch (d.getUnit()) {
        case DAY -> "Date32";
        case MILLISECOND -> "Date64";
      };
    }
    if (type instanceof ArrowType.Timestamp ts) {
      // Normalize to microseconds, preserving the timezone.
      return ts.getTimezone() == null
          ? "Timestamp(Microsecond)"
          : "Timestamp(Microsecond, \"" + ts.getTimezone() + "\")";
    }
    if (type instanceof ArrowType.Time t) {
      return t.getBitWidth() == 32 ? "Int32" : "Int64";
    }
    if (type instanceof ArrowType.Decimal d) {
      String kind = d.getBitWidth() == 256 ? "Decimal256" : "Decimal128";
      return kind + "(" + d.getPrecision() + ", " + d.getScale() + ")";
    }
    if (type instanceof ArrowType.Duration dur) {
      return "Duration(" + timeUnitName(dur.getUnit()) + ")";
    }
    if (type instanceof ArrowType.Interval iv) {
      return "Interval(" + intervalUnitName(iv.getUnit()) + ")";
    }
    if (type instanceof ArrowType.Null) {
      return "Null";
    }
    if (type instanceof ArrowType.List) {
      return "List(" + listChild(field.getChildren().get(0)) + ")";
    }
    if (type instanceof ArrowType.LargeList) {
      return "LargeList(" + listChild(field.getChildren().get(0)) + ")";
    }
    if (type instanceof ArrowType.FixedSizeList) {
      // Cast to a variable list: Spark can only read ArrayType from a ListVector. The element is
      // rendered cast-aware, so e.g. FixedSizeList<Float16> becomes List(Float32).
      return "List(" + listChild(field.getChildren().get(0)) + ")";
    }
    if (type instanceof ArrowType.Struct) {
      StringBuilder sb = new StringBuilder("Struct(");
      List<Field> children = field.getChildren();
      for (int i = 0; i < children.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(structField(children.get(i)));
      }
      return sb.append(")").toString();
    }
    if (type instanceof ArrowType.Map m) {
      Field entries = field.getChildren().get(0);
      return "Map("
          + structField(entries)
          + ", "
          + (m.getKeysSorted() ? "sorted" : "unsorted")
          + ")";
    }
    throw unsupported(field);
  }

  /** {@code <nullability><type>[, field: 'name']} -- the list/fixed-size-list child form. */
  private static String listChild(Field field) {
    String rendered = nullability(field) + renderArrowType(field);
    // The default list-field name ("item") is elided by the display form.
    return "item".equals(field.getName())
        ? rendered
        : rendered + ", field: '" + field.getName() + "'";
  }

  /** {@code "name": <nullability><type>} -- the struct/map field form. */
  private static String structField(Field field) {
    return debugQuote(field.getName()) + ": " + nullability(field) + renderArrowType(field);
  }

  private static String nullability(Field field) {
    return field.isNullable() ? "" : "non-null ";
  }

  private static String timeUnitName(TimeUnit unit) {
    return switch (unit) {
      case SECOND -> "Second";
      case MILLISECOND -> "Millisecond";
      case MICROSECOND -> "Microsecond";
      case NANOSECOND -> "Nanosecond";
    };
  }

  private static String intervalUnitName(IntervalUnit unit) {
    return switch (unit) {
      case YEAR_MONTH -> "YearMonth";
      case DAY_TIME -> "DayTime";
      case MONTH_DAY_NANO -> "MonthDayNano";
    };
  }

  /** Reproduce Rust's {@code {:?}} string quoting used by the Arrow display form. */
  private static String debugQuote(String s) {
    StringBuilder sb = new StringBuilder("\"");
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        default -> sb.append(c);
      }
    }
    return sb.append("\"").toString();
  }

  private static IllegalArgumentException unsupported(Field field) {
    return new IllegalArgumentException(
        "unsupported Arrow type for column '" + field.getName() + "': " + field.getType());
  }
}
