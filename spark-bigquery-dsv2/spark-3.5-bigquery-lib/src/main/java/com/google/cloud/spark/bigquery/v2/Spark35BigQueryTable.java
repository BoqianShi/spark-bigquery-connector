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
import com.google.cloud.spark.bigquery.v2.context.BigQueryDataSourceReaderContext;
import com.google.inject.Injector;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
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
        // Create the reader context to get table information
        BigQueryDataSourceReaderContext ctx = createBigQueryDataSourceReaderContext(options);
        TableInfo tableInfo = ctx.getTableInfo();
        
        if (tableInfo != null && SparkBigQueryUtil.isIcebergTable(tableInfo)) {
          // Try to load the Iceberg table using reflection to avoid compile-time dependency
          Optional<ScanBuilder> icebergScanBuilder = createIcebergScanBuilder(tableInfo, options);
          if (icebergScanBuilder.isPresent()) {
            return icebergScanBuilder.get();
          }
        }
      } catch (Exception e) {
        // Fall back to standard BigQuery read if Iceberg read fails
        // This is expected if Iceberg is not on the classpath or if there are configuration issues
      }
    }
    
    // Fall back to the standard BigQuery scan builder
    return super.newScanBuilder(options);
  }

  private Optional<ScanBuilder> createIcebergScanBuilder(
      TableInfo tableInfo, CaseInsensitiveStringMap options) {
    try {
      SparkSession spark = injector.getInstance(SparkSession.class);
      SparkBigQueryConfig config = injector.getInstance(SparkBigQueryConfig.class);
      
      // Get the Iceberg metadata location from the BigQuery table properties
      String metadataLocation = getIcebergMetadataLocation(tableInfo);
      if (metadataLocation == null || metadataLocation.isEmpty()) {
        return Optional.empty();
      }
      
      // Use reflection to load Iceberg table without compile-time dependency
      Class<?> hadoopTablesClass = Class.forName("org.apache.iceberg.hadoop.HadoopTables");
      Object hadoopTables = hadoopTablesClass.getConstructor().newInstance();
      
      // Load the Iceberg table
      Method loadMethod = hadoopTablesClass.getMethod("load", String.class);
      Object icebergTable = loadMethod.invoke(hadoopTables, metadataLocation);
      
      // Create SparkTable wrapper
      Class<?> sparkTableClass = Class.forName("org.apache.iceberg.spark.SparkTable");
      Class<?> tableClass = Class.forName("org.apache.iceberg.Table");
      Object sparkTable = sparkTableClass
          .getConstructor(tableClass, Boolean.TYPE)
          .newInstance(icebergTable, false);
      
      // Get the scan builder from the Iceberg table
      Method newScanBuilderMethod = sparkTableClass.getMethod(
          "newScanBuilder", CaseInsensitiveStringMap.class);
      ScanBuilder icebergScanBuilder = 
          (ScanBuilder) newScanBuilderMethod.invoke(sparkTable, options);
      
      return Optional.of(icebergScanBuilder);
    } catch (ClassNotFoundException e) {
      // Iceberg classes not available on classpath - expected when Iceberg is not provided
      return Optional.empty();
    } catch (Exception e) {
      // Any other error - fall back to standard read
      // This could be due to missing metadata location, permission issues, etc.
      return Optional.empty();
    }
  }

  private String getIcebergMetadataLocation(TableInfo tableInfo) {
    // BigQuery stores the Iceberg metadata location in the table's options/properties
    // The exact key depends on how BigQuery exposes Iceberg table metadata
    try {
      // Attempt to get metadata location from table options
      Map<String, String> tableOptions = tableInfo.getDefinition().toBuilder().build().toBuilder().build().toString();
      // This is a placeholder - the actual implementation would need to extract
      // the metadata location from BigQuery's table definition
      // Common patterns:
      // - tableInfo.getLabels().get("iceberg_metadata_location")
      // - tableInfo.getDefinition().getSchema().getFields() for metadata
      return null;
    } catch (Exception e) {
      return null;
    }
  }
}
