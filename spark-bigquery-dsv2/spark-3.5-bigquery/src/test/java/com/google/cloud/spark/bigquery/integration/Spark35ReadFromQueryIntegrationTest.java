/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.integration;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

import com.google.inject.ProvisionException;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;
import org.junit.Test;
public class Spark35ReadFromQueryIntegrationTest extends ReadFromQueryIntegrationTestBase {

  private static final String TEST_PROJECT_ID = "google.com:hadoop-cloud-dev";
  private static final String TEST_DATASET_ID = "boqian_dataset"; // REPLACE if not from base class
  private static final String TEST_TABLE_NAME = "parameter_types_test";
  private static final String COMPLEX_TABLE =
      TEST_DATASET_ID + ".array_struct_table";
  private static final String TEST_TABLE_ID =
      "`" + TEST_PROJECT_ID + "." + TEST_DATASET_ID + "." + TEST_TABLE_NAME + "`";

  private static final List<String> TEST_COLUMN_NAMES;
  private static final Map<String, String> EXPECTED_PARAM_VALUES;
  private static final Map<String, Object> EXPECTED_ROW_VALUES;
  private static final String ALL_TYPES_NAMED_QUERY;
  private static final String ALL_TYPES_POSITIONAL_QUERY;

  static {
    List<String> columns = Arrays.asList(
        "id", "bool_col", "int64_col", "float64_col", "numeric_col", "string_col",
        "bytes_col", "date_col", "geo_col", "json_col", "time_col",
        "timestamp_col", "datetime_col");
    TEST_COLUMN_NAMES = Collections.unmodifiableList(new ArrayList<String>(columns));

    Map<String, String> paramMap = new HashMap<>();
    paramMap.put("id",          "INT64:1");
    paramMap.put("bool_col",    "BOOL:true");
    paramMap.put("int64_col",   "INT64:9876543210");
    paramMap.put("float64_col", "FLOAT64:12345.6789");
    paramMap.put("numeric_col", "NUMERIC:12345678901234567890123456789.123456789");
    paramMap.put("string_col",  "STRING:hello parameter world");    paramMap.put("bytes_col",   "BYTES:dGVzdF9ieXRlcw==");
    paramMap.put("date_col",    "DATE:2023-11-15");
    paramMap.put("geo_col",     "GEOGRAPHY:POINT(-122.35 37.42)");
    paramMap.put("json_col",    "JSON:{\"name\": \"test\", \"value\": 100}");
    paramMap.put("time_col",    "TIME:14:35:10.123456");
    paramMap.put("timestamp_col", "TIMESTAMP:2023-11-15 14:35:10.123456Z");
    paramMap.put("datetime_col", "DATETIME:2023-11-15 14:35:10.123456");
    EXPECTED_PARAM_VALUES = Collections.unmodifiableMap(paramMap);

    Map<String, Object> rowMap = new HashMap<>();
    rowMap.put("id",          1L);
    rowMap.put("bool_col",    true);
    rowMap.put("int64_col",   9876543210L);
    rowMap.put("float64_col", 12345.6789);
    rowMap.put("numeric_col", new BigDecimal("12345678901234567890123456789.123456789"));
    rowMap.put("string_col",  "hello parameter world");
    rowMap.put("bytes_col",   Base64.getDecoder().decode("dGVzdF9ieXRlcw=="));
    rowMap.put("date_col",    Date.valueOf("2023-11-15"));
    rowMap.put("geo_col",     "POINT(-122.35 37.42)");
    rowMap.put("json_col",    "{\"name\":\"test\",\"value\":100}");
    long expectedMicroseconds = (14L * 3600 * 1000000) + (35 * 60 * 1000000) + (10 * 1000000) + 123456L;
    rowMap.put("time_col",    expectedMicroseconds);
    rowMap.put("timestamp_col", Timestamp.from(Instant.parse("2023-11-15T14:35:10.123456Z")));
    rowMap.put("datetime_col", LocalDateTime.parse("2023-11-15T14:35:10.123456"));
    EXPECTED_ROW_VALUES = Collections.unmodifiableMap(rowMap);

    StringBuilder namedWhere = new StringBuilder();
    StringBuilder positionalWhere = new StringBuilder();
    for (int i = 0; i < TEST_COLUMN_NAMES.size(); i++) {
      String colName = TEST_COLUMN_NAMES.get(i);
      if (i > 0) {
        namedWhere.append(" AND ");
        positionalWhere.append(" AND ");
      }
      if ("geo_col".equals(colName)) {
        namedWhere.append("ST_EQUALS(").append(colName).append(", @").append(colName).append(")");
        positionalWhere.append("ST_EQUALS(").append(colName).append(", ?").append(")");
      } else if ("json_col".equals(colName)) {
        namedWhere.append("TO_JSON_STRING(").append(colName).append(") = TO_JSON_STRING(@").append(colName).append(")");
        positionalWhere.append("TO_JSON_STRING(").append(colName).append(") = TO_JSON_STRING(?)");
      }
      else {
        namedWhere.append(colName).append(" = @").append(colName);
        positionalWhere.append(colName).append(" = ?");
      }
    }

    ALL_TYPES_NAMED_QUERY = "SELECT * FROM " + TEST_TABLE_ID + " WHERE " + namedWhere.toString();
    ALL_TYPES_POSITIONAL_QUERY = "SELECT * FROM " + TEST_TABLE_ID + " WHERE " + positionalWhere.toString();
  }

  public Spark35ReadFromQueryIntegrationTest() {
    super(true);
  }


  @Test
  public void testReadWithAllTypesNamedParams() {
    Map<String, String> options = new HashMap<>();
    options.put("query", ALL_TYPES_NAMED_QUERY);
    options.put("viewsEnabled", "true");

    for (Map.Entry<String, String> entry : EXPECTED_PARAM_VALUES.entrySet()) {
      options.put("NamedParameters." + entry.getKey(), entry.getValue());
    }

    Dataset<Row> df = spark.read().format("bigquery").options(options).load();

    assertThat(df.count()).isEqualTo(1L);

    StructType schema = df.schema();
    assertThat(schema.fieldNames()).asList().containsExactlyElementsIn(TEST_COLUMN_NAMES).inOrder();
    assertThat(schema.apply("id").dataType()).isEqualTo(DataTypes.LongType);
    assertThat(schema.apply("bool_col").dataType()).isEqualTo(DataTypes.BooleanType);
    assertThat(schema.apply("numeric_col").dataType()).isInstanceOf(DecimalType.class);
    assertThat(schema.apply("bytes_col").dataType()).isEqualTo(DataTypes.BinaryType);
    assertThat(schema.apply("date_col").dataType()).isEqualTo(DataTypes.DateType);
    assertThat(schema.apply("geo_col").dataType()).isEqualTo(DataTypes.StringType);
    assertThat(schema.apply("json_col").dataType()).isEqualTo(DataTypes.StringType);
    assertThat(schema.apply("time_col").dataType()).isEqualTo(DataTypes.LongType);
    assertThat(schema.apply("timestamp_col").dataType()).isEqualTo(DataTypes.TimestampType);
    assertThat(schema.apply("datetime_col").dataType()).isEqualTo(DataTypes.TimestampNTZType);

    Row resultRow = df.first();
    for (String colName : TEST_COLUMN_NAMES) {
      Object expectedValue = EXPECTED_ROW_VALUES.get(colName);
      Object actualValue = resultRow.getAs(colName);

      if (expectedValue instanceof byte[]) {
        assertArrayEquals("Byte array mismatch for column: " + colName, (byte[]) expectedValue, (byte[]) actualValue);
      } else if (expectedValue instanceof BigDecimal) {
        assertThat(((BigDecimal) actualValue).compareTo((BigDecimal) expectedValue)).isEqualTo(0);
      } else {
        assertThat(actualValue).isEqualTo(expectedValue);
      }
    }
  }

  @Test
  public void testReadWithAllTypesPositionalParams() {
    Map<String, String> options = new HashMap<>();
    options.put("query", ALL_TYPES_POSITIONAL_QUERY);
    options.put("viewsEnabled", "true");

    for (int i = 0; i < TEST_COLUMN_NAMES.size(); i++) {
      String colName = TEST_COLUMN_NAMES.get(i);
      String paramValue = EXPECTED_PARAM_VALUES.get(colName);
      options.put("PositionalParameters." + (i + 1), paramValue);
    }

    Dataset<Row> df = spark.read().format("bigquery").options(options).load();


    assertThat(df.count()).isEqualTo(1L);

    StructType schema = df.schema();
    assertThat(schema.fieldNames()).asList().containsExactlyElementsIn(TEST_COLUMN_NAMES).inOrder();
    assertThat(schema.apply("id").dataType()).isEqualTo(DataTypes.LongType);
    assertThat(schema.apply("bool_col").dataType()).isEqualTo(DataTypes.BooleanType);
    assertThat(schema.apply("numeric_col").dataType()).isInstanceOf(DecimalType.class);
    assertThat(schema.apply("bytes_col").dataType()).isEqualTo(DataTypes.BinaryType);
    assertThat(schema.apply("date_col").dataType()).isEqualTo(DataTypes.DateType);
    assertThat(schema.apply("geo_col").dataType()).isEqualTo(DataTypes.StringType);
    assertThat(schema.apply("json_col").dataType()).isEqualTo(DataTypes.StringType);
    assertThat(schema.apply("time_col").dataType()).isEqualTo(DataTypes.LongType);
    assertThat(schema.apply("timestamp_col").dataType()).isEqualTo(DataTypes.TimestampType);
    assertThat(schema.apply("datetime_col").dataType()).isEqualTo(DataTypes.TimestampNTZType);

    Row resultRow = df.first();
    for (String colName : TEST_COLUMN_NAMES) {
      Object expectedValue = EXPECTED_ROW_VALUES.get(colName);
      Object actualValue = resultRow.getAs(colName);

      if (expectedValue instanceof byte[]) {
        assertArrayEquals("Byte array mismatch for column: " + colName, (byte[]) expectedValue, (byte[]) actualValue);
      } else if (expectedValue instanceof BigDecimal) {
        assertThat(((BigDecimal) actualValue).compareTo((BigDecimal) expectedValue)).isEqualTo(0);
      } else {
        assertThat(actualValue).isEqualTo(expectedValue);
      }
    }
  }

  @Test
  public void testReadWithMixedParametersFails() {
    String queryForMixedTest = "SELECT * FROM " + TEST_TABLE_ID + " WHERE id = @id OR id = ?";

    ProvisionException thrown = assertThrows(
        ProvisionException.class,
        () -> {
          spark.read()
              .format("bigquery")
              .option("query", queryForMixedTest)
              .option("viewsEnabled", "true")
              .option("NamedParameters.id", "INT64:1")
              .option("PositionalParameters.1", "INT64:2")
              .load()
              .show();
        });

    Throwable cause = thrown.getCause();
    assertThat(cause).isNotNull();
    assertThat(cause).isInstanceOf(IllegalArgumentException.class);
    assertThat(cause)
        .hasMessageThat()
        .contains("Cannot mix NamedParameters.* and PositionalParameters.* options.");
  }

  // ---- NEW TEST FOR COMPLEX TYPES ----
  @Test
  public void testReadComplexTypes() {
    Dataset<Row> df = spark
        .read()
        .format("bigquery")
        .option("table", COMPLEX_TABLE)
        .load();

    df.printSchema();
    // root
    //  |-- id: long (nullable = false) // BQ INT64 NOT NULL -> Spark long (nullable=false) - Check connector behaviour! Might be true.
    //  |-- description: string (nullable = true)
    //  |-- int_array: array (nullable = true)
    //  |    |-- element: long (containsNull = true) // BQ ARRAY<INT64> -> Spark ArrayType(LongType)
    //  |-- string_array: array (nullable = true)
    //  |    |-- element: string (containsNull = true) // BQ ARRAY<STRING> -> Spark ArrayType(StringType)
    //  |-- simple_struct: struct (nullable = true)
    //  |    |-- name: string (nullable = true)       // BQ STRUCT<name STRING, age INT64>
    //  |    |-- age: long (nullable = true)          // -> Spark StructType(...)
    //  |-- nested_struct: struct (nullable = true)
    //  |    |-- id: string (nullable = true)
    //  |    |-- details: struct (nullable = true)
    //  |    |    |-- status: string (nullable = true)
    //  |    |    |-- value: double (nullable = true) // BQ FLOAT64 -> Spark DoubleType
    //  |-- interval_col: calendarinterval (nullable = true) // BQ INTERVAL -> Spark CalendarIntervalType
    //  |-- date_range: struct (nullable = true)        // BQ RANGE<DATE> -> Spark StructType
    //  |    |-- start: date (nullable = true)          // Default field names 'start', 'end' assumed
    //  |    |-- end: date (nullable = true)
    //  |-- ts_range: struct (nullable = true)          // BQ RANGE<TIMESTAMP> -> Spark StructType
    //  |    |-- start: timestamp (nullable = true)
    //  |    |-- end: timestamp (nullable = true)

    StructType expectedSchema = new StructType()
        .add("id", DataTypes.LongType,
            false) // Assuming NOT NULL maps to nullable=false. Verify this!
        .add("description", DataTypes.StringType, true)
        .add("int_array", DataTypes.createArrayType(DataTypes.LongType, true), true)
        .add("string_array", DataTypes.createArrayType(DataTypes.StringType, true), true)
        .add("simple_struct", new StructType()
            .add("name", DataTypes.StringType, true)
            .add("age", DataTypes.LongType, true), true)
        .add("nested_struct", new StructType()
            .add("id", DataTypes.StringType, true)
            .add("details", new StructType()
                .add("status", DataTypes.StringType, true)
                .add("value", DataTypes.DoubleType, true), true), true);

    // Compare schemas (normalize for potential slight differences if needed)
    // Using simple equality check first
    assertThat(df.schema()).isEqualTo(expectedSchema);

    // Basic data check (optional, but good)
    assertThat(df.count()).isEqualTo(1); // We inserted 4 rows

    // Spot check first row (ID=1)
    Row row1 = df.filter("id = 1").first();
    assertThat(row1.getLong(0)).isEqualTo(1L); // id
    assertThat(row1.getString(1)).isEqualTo("All Populated"); // description
    assertThat(row1.<Long>getList(2)).containsExactly(10L, 20L, 30L).inOrder(); // int_array
    assertThat(row1.<String>getList(3)).containsExactly("apple", "banana", "cherry")
        .inOrder(); // string_array
    Row simpleStruct1 = row1.getStruct(4);
    assertThat(simpleStruct1.getString(0)).isEqualTo("Alice"); // simple_struct.name
    assertThat(simpleStruct1.getLong(1)).isEqualTo(30L); // simple_struct.age
  }

  // tests are from the super-class

}
