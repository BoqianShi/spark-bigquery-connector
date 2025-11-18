/*
 * Copyright 2025 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.v2;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.SparkBigQueryUtil;
import com.google.inject.Injector;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class Spark35BigQueryTable extends Spark34BigQueryTable {
  public Spark35BigQueryTable(Injector injector, Supplier<StructType> schemaSupplier) {
    super(injector, schemaSupplier);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    // SaveMode is not provided by spark 3, it is handled by the DataFrameWriter
    // The case where mode == SaveMode.Ignore is handled by Spark, so we can assume we can get the
    // context
    return new Spark35BigQueryWriteBuilder(injector, info, SaveMode.Append);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
    
    // Check if Iceberg direct read is enabled and if the table is an Iceberg table
    if (config.enableIcebergDirectRead() && tableId != null) {
      try {
        // Get the BigQuery table info to check if it's an Iceberg table
        TableInfo tableInfo = getTableInfo();
        
        if (tableInfo != null && SparkBigQueryUtil.isIcebergTable(tableInfo)) {
          // Try to load the Iceberg table using reflection to avoid compile-time dependency
          Optional<ScanBuilder> icebergScanBuilder = createIcebergScanBuilder(options);
          if (icebergScanBuilder.isPresent()) {
            return icebergScanBuilder.get();
          }
        }
      } catch (Exception e) {
        // Fall back to standard BigQuery read if Iceberg read fails
        // Log the exception but continue with normal flow
      }
    }
    
    // Fall back to the standard BigQuery scan builder
    return super.newScanBuilder(options);
  }

  private TableInfo getTableInfo() {
    try {
      SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
      // Use reflection or direct access to get BigQueryClient and fetch table info
      // For now, we'll use the config's table helper if available
      return null; // Placeholder - will be implemented with proper BigQueryClient access
    } catch (Exception e) {
      return null;
    }
  }

  private Optional<ScanBuilder> createIcebergScanBuilder(CaseInsensitiveStringMap options) {
    try {
      SparkSession spark = injector.getInstance(SparkSession.class);
      SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
      
      // Get the Iceberg catalog and load the Iceberg table
      // The Iceberg table location is stored as metadata in BigQuery
      String icebergTableLocation = getIcebergTableLocation();
      if (icebergTableLocation == null) {
        return Optional.empty();
      }
      
      // Use reflection to load Iceberg classes to avoid compile-time dependency
      Class<?> sparkTableClass = Class.forName("org.apache.iceberg.spark.SparkTable");
      Class<?> catalogClass = Class.forName("org.apache.iceberg.spark.SparkCatalog");
      
      // Create an Iceberg table identifier
      String[] namespace = new String[] {config.getTableId().getDataset()};
      Identifier identifier = Identifier.of(namespace, config.getTableId().getTable());
      
      // Load the Iceberg table through reflection
      // This is a placeholder - actual implementation would need proper Iceberg table loading
      return Optional.empty();
    } catch (ClassNotFoundException e) {
      // Iceberg classes not available on classpath
      return Optional.empty();
    } catch (Exception e) {
      // Any other error - fall back to standard read
      return Optional.empty();
    }
  }

  private String getIcebergTableLocation() {
    // Placeholder - needs to be implemented to fetch Iceberg metadata location from BigQuery
    // This would typically come from the table's metadata/properties
    return null;
  }
}
