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

import org.apache.datafusion.protobuf.BinaryExprNode;
import org.apache.datafusion.protobuf.IsNotNull;
import org.apache.datafusion.protobuf.IsNull;
import org.apache.datafusion.protobuf.LogicalExprNode;
import org.apache.datafusion.protobuf.Not;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Or;

import datafusion_common.DatafusionCommon.Column;
import datafusion_common.DatafusionCommon.ScalarValue;

/**
 * Translates Spark {@link Filter}s into serialized {@code datafusion.LogicalExprNode} bytes for
 * filter pushdown.
 *
 * <p>Translates the comparison, boolean, and null predicates over primitive literals that map
 * cleanly; anything else is reported as not pushed so Spark applies it itself. A translated filter
 * is applied exactly by DataFusion (the scan core calls {@code DataFrame::filter}), so it is safe
 * to treat it as fully handled.
 */
final class SparkFilters {

  private SparkFilters() {}

  /** Pushed filter bytes, and the filters Spark must still apply itself. */
  record Result(List<byte[]> pushed, Filter[] pushedFilters, Filter[] postScan) {}

  static Result split(Filter[] filters) {
    List<byte[]> pushed = new ArrayList<>();
    List<Filter> pushedFilters = new ArrayList<>();
    List<Filter> postScan = new ArrayList<>();
    for (Filter filter : filters) {
      LogicalExprNode expr = translate(filter);
      if (expr != null) {
        pushed.add(expr.toByteArray());
        pushedFilters.add(filter);
      } else {
        postScan.add(filter);
      }
    }
    return new Result(
        pushed, pushedFilters.toArray(new Filter[0]), postScan.toArray(new Filter[0]));
  }

  /** Translate a single filter, or return null if it cannot be expressed. */
  private static LogicalExprNode translate(Filter filter) {
    if (filter instanceof EqualTo f) {
      return binary("Eq", f.attribute(), f.value());
    }
    if (filter instanceof GreaterThan f) {
      return binary("Gt", f.attribute(), f.value());
    }
    if (filter instanceof GreaterThanOrEqual f) {
      return binary("GtEq", f.attribute(), f.value());
    }
    if (filter instanceof LessThan f) {
      return binary("Lt", f.attribute(), f.value());
    }
    if (filter instanceof LessThanOrEqual f) {
      return binary("LtEq", f.attribute(), f.value());
    }
    if (filter instanceof org.apache.spark.sql.sources.IsNull f) {
      return wrap(b -> b.setIsNullExpr(IsNull.newBuilder().setExpr(column(f.attribute()))));
    }
    if (filter instanceof org.apache.spark.sql.sources.IsNotNull f) {
      return wrap(b -> b.setIsNotNullExpr(IsNotNull.newBuilder().setExpr(column(f.attribute()))));
    }
    if (filter instanceof And f) {
      LogicalExprNode l = translate(f.left());
      LogicalExprNode r = translate(f.right());
      return (l == null || r == null) ? null : binaryNodes("And", l, r);
    }
    if (filter instanceof Or f) {
      LogicalExprNode l = translate(f.left());
      LogicalExprNode r = translate(f.right());
      return (l == null || r == null) ? null : binaryNodes("Or", l, r);
    }
    if (filter instanceof org.apache.spark.sql.sources.Not f) {
      LogicalExprNode child = translate(f.child());
      return child == null ? null : wrap(b -> b.setNotExpr(Not.newBuilder().setExpr(child)));
    }
    return null;
  }

  private static LogicalExprNode binary(String op, String attribute, Object value) {
    ScalarValue literal = scalar(value);
    if (literal == null) {
      return null;
    }
    return binaryNodes(
        op, column(attribute), LogicalExprNode.newBuilder().setLiteral(literal).build());
  }

  private static LogicalExprNode binaryNodes(
      String op, LogicalExprNode left, LogicalExprNode right) {
    return LogicalExprNode.newBuilder()
        .setBinaryExpr(BinaryExprNode.newBuilder().addOperands(left).addOperands(right).setOp(op))
        .build();
  }

  private static LogicalExprNode column(String attribute) {
    return LogicalExprNode.newBuilder().setColumn(Column.newBuilder().setName(attribute)).build();
  }

  private interface ExprFiller {
    LogicalExprNode.Builder apply(LogicalExprNode.Builder builder);
  }

  private static LogicalExprNode wrap(ExprFiller filler) {
    return filler.apply(LogicalExprNode.newBuilder()).build();
  }

  /** Map a Spark literal to a DataFusion ScalarValue, or null if unsupported. */
  private static ScalarValue scalar(Object value) {
    if (value instanceof Long v) {
      return ScalarValue.newBuilder().setInt64Value(v).build();
    }
    if (value instanceof Integer v) {
      return ScalarValue.newBuilder().setInt32Value(v).build();
    }
    if (value instanceof Double v) {
      return ScalarValue.newBuilder().setFloat64Value(v).build();
    }
    if (value instanceof Float v) {
      return ScalarValue.newBuilder().setFloat32Value(v).build();
    }
    if (value instanceof Boolean v) {
      return ScalarValue.newBuilder().setBoolValue(v).build();
    }
    if (value instanceof String v) {
      return ScalarValue.newBuilder().setUtf8Value(v).build();
    }
    return null;
  }
}
