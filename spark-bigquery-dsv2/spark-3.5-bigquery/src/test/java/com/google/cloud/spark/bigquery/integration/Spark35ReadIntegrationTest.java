/*
 * Copyright 2021 Google LLC // Keep original copyright
 * Modified 2024 Google LLC // Add modification notice if appropriate
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
import static org.junit.Assert.assertThrows; // Assuming JUnit 4 style based on original

import com.google.inject.ProvisionException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class Spark35ReadIntegrationTest extends ReadIntegrationTestBase {

  public Spark35ReadIntegrationTest() {
    // Keep constructor as is
    super(/* userProvidedSchemaAllowed */ false, DataTypes.TimestampNTZType);
  }
  private static final String USER_DATA_TABLE = "`google.com:hadoop-cloud-dev.boqian_eu_test.sample_user_data`";
  private static final String NORMAL_USER_QUERY =
      "SELECT user_name, registration_date FROM " + USER_DATA_TABLE +
          " WHERE is_active = true AND registration_date >= DATE('2024-01-01') " +
          " ORDER BY registration_date ASC";

  @Test
  public void testReadWithNormalQueryUS() { // Renamed original test
    Dataset<Row> df = spark
        .read()
        .format("bigquery")
        .option("location", "US")
        .option("query", NORMAL_USER_QUERY) // No parameters in query or options
        .option("viewsEnabled", "true")
        .load();

    df.printSchema();
    df.show();
  }
  @Test
  public void testReadWithNormalQueryEU() { // Renamed original test
    Dataset<Row> df = spark
        .read()
        .format("bigquery")
        .option("location", "EU")
        .option("query", NORMAL_USER_QUERY) // No parameters in query or options
        .option("viewsEnabled", "true")
        .load();

    df.printSchema();
    df.show();
  }

}