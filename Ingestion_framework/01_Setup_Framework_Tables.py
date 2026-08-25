# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: Framework Tables
# MAGIC This notebook bootstraps the ingestion framework:
# MAGIC 1. Creates the `framework` schema
# MAGIC 2. Creates the `ingestion_config` and `ingestion_audit` control tables
# MAGIC 3. Loads/merges configuration records from a CSV file into `ingestion_config`
# MAGIC
# MAGIC Run this notebook once during initial setup, and again any time the
# MAGIC configuration CSV changes.

# COMMAND ----------

# ============================================================
# CELL 1: BOOTSTRAP LOCATION
# ============================================================

# This is the only fixed location.
# The framework must know where its configuration table exists.

FRAMEWORK_CATALOG = "workspace"
FRAMEWORK_SCHEMA = "framework"

CONFIG_TABLE = (
    f"{FRAMEWORK_CATALOG}."
    f"{FRAMEWORK_SCHEMA}."
    f"ingestion_config"
)

AUDIT_TABLE = (
    f"{FRAMEWORK_CATALOG}."
    f"{FRAMEWORK_SCHEMA}."
    f"ingestion_audit"
)

print("Configuration table:", CONFIG_TABLE)
print("Audit table:", AUDIT_TABLE)

# COMMAND ----------

# ============================================================
# CELL 2: CREATE FRAMEWORK SCHEMA
# ============================================================

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS
    {FRAMEWORK_CATALOG}.{FRAMEWORK_SCHEMA}
    """
)

print(
    f"Framework schema created: "
    f"{FRAMEWORK_CATALOG}.{FRAMEWORK_SCHEMA}"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS workspace.framework.ingestion_config;
# MAGIC
# MAGIC CREATE TABLE workspace.framework.ingestion_config
# MAGIC (
# MAGIC     config_id BIGINT,
# MAGIC     source_name STRING,
# MAGIC     file_name_pattern STRING,
# MAGIC     file_format STRING,
# MAGIC     delimiter_name STRING,
# MAGIC     header_flag BOOLEAN,
# MAGIC     infer_schema_flag BOOLEAN,
# MAGIC
# MAGIC     source_path STRING,
# MAGIC
# MAGIC     target_catalog STRING,
# MAGIC     target_schema STRING,
# MAGIC     target_table STRING,
# MAGIC
# MAGIC     load_type STRING,
# MAGIC
# MAGIC     archive_path STRING,
# MAGIC     error_path STRING,
# MAGIC
# MAGIC     active_flag BOOLEAN,
# MAGIC     created_timestamp TIMESTAMP,
# MAGIC     updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS workspace.framework.ingestion_audit;
# MAGIC
# MAGIC CREATE TABLE workspace.framework.ingestion_audit
# MAGIC (
# MAGIC     audit_id STRING,
# MAGIC     run_id STRING,
# MAGIC     config_id BIGINT,
# MAGIC     source_name STRING,
# MAGIC     source_file STRING,
# MAGIC     source_path STRING,
# MAGIC     target_table STRING,
# MAGIC     load_type STRING,
# MAGIC     start_timestamp TIMESTAMP,
# MAGIC     end_timestamp TIMESTAMP,
# MAGIC     records_read BIGINT,
# MAGIC     records_written BIGINT,
# MAGIC     status STRING,
# MAGIC     error_message STRING,
# MAGIC     processed_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# ============================================================
# LOAD CONFIGURATIONS FROM CSV AND MERGE INTO CONFIG TABLE
# ============================================================

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType
)

CONFIG_FILE_PATH = (
    "/Volumes/workspace/bronze/landing_raw_data/framework_files/config_file.csv"
)

CONFIG_TABLE = "workspace.framework.ingestion_config"


# Define the expected configuration schema.
config_schema = StructType([
    StructField("config_id", LongType(), False),
    StructField("source_name", StringType(), False),
    StructField("file_name_pattern", StringType(), False),
    StructField("file_format", StringType(), False),
    StructField("delimiter_name", StringType(), True),
    StructField("header_flag", BooleanType(), True),
    StructField("infer_schema_flag", BooleanType(), True),
    StructField("source_path", StringType(), False),
    StructField("target_catalog", StringType(), False),
    StructField("target_schema", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("load_type", StringType(), False),
    StructField("archive_path", StringType(), False),
    StructField("error_path", StringType(), False),
    StructField("active_flag", BooleanType(), False)
])


# Read all configurations from the CSV file.
incoming_config_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(config_schema)
    .load(CONFIG_FILE_PATH)
)


# Clean and standardize values.
incoming_config_df = (
    incoming_config_df
    .withColumn(
        "source_name",
        F.upper(F.trim(F.col("source_name")))
    )
    .withColumn(
        "file_format",
        F.upper(F.trim(F.col("file_format")))
    )
    .withColumn(
        "delimiter_name",
        F.upper(F.trim(F.col("delimiter_name")))
    )
    .withColumn(
        "load_type",
        F.upper(F.trim(F.col("load_type")))
    )
    .withColumn(
        "source_path",
        F.trim(F.col("source_path"))
    )
    .withColumn(
        "archive_path",
        F.trim(F.col("archive_path"))
    )
    .withColumn(
        "error_path",
        F.trim(F.col("error_path"))
    )
)


# Remove duplicate config IDs from the incoming CSV.
incoming_config_df = (
    incoming_config_df
    .dropDuplicates(["config_id"])
)


# Validate required fields.
invalid_config_df = incoming_config_df.filter(
    F.col("config_id").isNull()
    | F.col("source_name").isNull()
    | F.col("file_name_pattern").isNull()
    | F.col("file_format").isNull()
    | F.col("source_path").isNull()
    | F.col("target_catalog").isNull()
    | F.col("target_schema").isNull()
    | F.col("target_table").isNull()
    | F.col("load_type").isNull()
)


if invalid_config_df.count() > 0:

    print("Invalid configuration records were found.")

    display(invalid_config_df)

    raise ValueError(
        "Configuration file contains missing required values."
    )


# Validate load types.
invalid_load_type_df = incoming_config_df.filter(
    ~F.col("load_type").isin("APPEND", "OVERWRITE")
)

if invalid_load_type_df.count() > 0:

    display(invalid_load_type_df)

    raise ValueError(
        "Unsupported load type found. "
        "Only APPEND and OVERWRITE are supported."
    )


# Add timestamps for insert and update.
source_config_df = (
    incoming_config_df
    .withColumn(
        "created_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "updated_timestamp",
        F.current_timestamp()
    )
)


# Merge configurations into the Delta table.
target_config_table = DeltaTable.forName(
    spark,
    CONFIG_TABLE
)

(
    target_config_table.alias("target")
    .merge(
        source_config_df.alias("source"),
        "target.config_id = source.config_id"
    )
    .whenMatchedUpdate(
        set={
            "source_name": "source.source_name",
            "file_name_pattern": "source.file_name_pattern",
            "file_format": "source.file_format",
            "delimiter_name": "source.delimiter_name",
            "header_flag": "source.header_flag",
            "infer_schema_flag": "source.infer_schema_flag",
            "source_path": "source.source_path",
            "target_catalog": "source.target_catalog",
            "target_schema": "source.target_schema",
            "target_table": "source.target_table",
            "load_type": "source.load_type",
            "archive_path": "source.archive_path",
            "error_path": "source.error_path",
            "active_flag": "source.active_flag",
            "updated_timestamp": "current_timestamp()"
        }
    )
    .whenNotMatchedInsert(
        values={
            "config_id": "source.config_id",
            "source_name": "source.source_name",
            "file_name_pattern": "source.file_name_pattern",
            "file_format": "source.file_format",
            "delimiter_name": "source.delimiter_name",
            "header_flag": "source.header_flag",
            "infer_schema_flag": "source.infer_schema_flag",
            "source_path": "source.source_path",
            "target_catalog": "source.target_catalog",
            "target_schema": "source.target_schema",
            "target_table": "source.target_table",
            "load_type": "source.load_type",
            "archive_path": "source.archive_path",
            "error_path": "source.error_path",
            "active_flag": "source.active_flag",
            "created_timestamp": "current_timestamp()",
            "updated_timestamp": "current_timestamp()"
        }
    )
    .execute()
)

print("Configuration table updated successfully.")

display(
    spark.table(CONFIG_TABLE)
    .orderBy("config_id")
)
