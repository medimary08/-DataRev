# Databricks notebook source
# MAGIC %md
# MAGIC # Generic Metadata-Driven Ingestion Framework
# MAGIC
# MAGIC Reads active rows from `workspace.framework.ingestion_config`, discovers
# MAGIC matching source files, loads each into its configured Delta target table,
# MAGIC writes an audit trail to `workspace.framework.ingestion_audit`, and
# MAGIC archives (or quarantines) each processed file.
# MAGIC
# MAGIC Run `01_Setup_Framework_Tables` first to create the control tables and
# MAGIC load configuration rows.

# COMMAND ----------

# ============================================================
# CELL 1: IMPORT LIBRARIES
# ============================================================

from datetime import datetime
from fnmatch import fnmatch
from uuid import uuid4

from pyspark.sql import functions as F

print("Libraries imported successfully.")

# COMMAND ----------

# ============================================================
# CELL 2: FRAMEWORK TABLE LOCATIONS
# ============================================================

CONFIG_TABLE = "workspace.framework.ingestion_config"

AUDIT_TABLE = "workspace.framework.ingestion_audit"

print("Configuration table:", CONFIG_TABLE)
print("Audit table:", AUDIT_TABLE)

# COMMAND ----------

# ============================================================
# CELL 3: READ ACTIVE CONFIGURATIONS
# ============================================================

config_df = (
    spark.table(CONFIG_TABLE)
    .filter(F.col("active_flag") == True)
    .orderBy("config_id")
)

print("Number of active configurations:", config_df.count())

display(config_df)

# COMMAND ----------

# ============================================================
# CELL 4: CREATE FRAMEWORK RUN ID
# ============================================================

RUN_ID = str(uuid4())

print("Framework run ID:", RUN_ID)

# COMMAND ----------

# ============================================================
# CELL 5: DELIMITER MAPPING
# ============================================================

def get_delimiter(delimiter_value):
    """
    Convert a configured delimiter name into its actual character.
    """

    if delimiter_value is None:
        raise ValueError("Delimiter is missing in configuration.")

    delimiter_mapping = {
        "COMMA": ",",
        "PIPE": "|",
        "TAB": "\t",
        "SEMICOLON": ";"
    }

    cleaned_value = delimiter_value.strip().upper()

    # Configuration contains a friendly name.
    if cleaned_value in delimiter_mapping:
        return delimiter_mapping[cleaned_value]

    # Configuration already contains the actual character.
    if delimiter_value in [",", "|", "\t", ";"]:
        return delimiter_value

    raise ValueError(
        f"Unsupported delimiter: {delimiter_value}"
    )

# COMMAND ----------

# ============================================================
# CELL 6: CHECK PATH EXISTS
# ============================================================

def validate_path_exists(path_value, path_type):
    """
    Validate that a configured path exists.

    The function does not create the path.
    It fails when the path is unavailable.
    """

    if path_value is None or not path_value.strip():
        raise ValueError(
            f"{path_type} path is missing in configuration."
        )

    path_value = path_value.strip().rstrip("/")

    try:
        dbutils.fs.ls(path_value)

    except Exception as error:
        raise FileNotFoundError(
            f"{path_type} path does not exist or cannot be accessed: "
            f"{path_value}. Error: {str(error)}"
        )

    return path_value

# COMMAND ----------

def find_matching_files(source_path, file_name_pattern):
    """
    Find files matching the configured filename pattern.
    """

    source_path = validate_path_exists(
        source_path,
        "Source"
    )

    if (
        file_name_pattern is None
        or not file_name_pattern.strip()
    ):
        raise ValueError(
            "file_name_pattern is missing in configuration."
        )

    all_items = dbutils.fs.ls(source_path)

    matching_files = []

    for item in all_items:

        if item.path.endswith("/"):
            continue

        if fnmatch(
            item.name.lower(),
            file_name_pattern.strip().lower()
        ):
            matching_files.append(item)

    return matching_files

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType
)


def write_audit_record(
    audit_id,
    run_id,
    config_id,
    source_name,
    source_file,
    source_path,
    target_table,
    load_type,
    start_timestamp,
    end_timestamp,
    records_read,
    records_written,
    status,
    error_message
):
    """
    Insert one processing record into the audit table.
    """

    audit_schema = StructType([
        StructField("audit_id", StringType(), True),
        StructField("run_id", StringType(), True),
        StructField("config_id", LongType(), True),
        StructField("source_name", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("source_path", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("load_type", StringType(), True),
        StructField("start_timestamp", TimestampType(), True),
        StructField("end_timestamp", TimestampType(), True),
        StructField("records_read", LongType(), True),
        StructField("records_written", LongType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("processed_timestamp", TimestampType(), True)
    ])

    audit_data = [(
        audit_id,
        run_id,
        int(config_id),
        source_name,
        source_file,
        source_path,
        target_table,
        load_type,
        start_timestamp,
        end_timestamp,
        int(records_read),
        int(records_written),
        status,
        error_message,
        datetime.now()
    )]

    audit_df = spark.createDataFrame(
        audit_data,
        schema=audit_schema
    )

    (
        audit_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(AUDIT_TABLE)
    )

# COMMAND ----------

# ============================================================
# CELL 11: READ SOURCE FILE FUNCTION
# ============================================================

def read_source_file(
    file_path,
    file_format,
    delimiter,
    header_flag,
    infer_schema_flag
):
    """
    Read the source file using configuration-table instructions.
    """

    normalized_format = file_format.strip().lower()

    if normalized_format == "csv":

        source_df = (
            spark.read
            .format("csv")
            .option("header", str(header_flag).lower())
            .option("inferSchema", str(infer_schema_flag).lower())
            .option("delimiter", delimiter)
            .option("mode", "PERMISSIVE")
            .load(file_path)
        )

        return source_df

    elif normalized_format == "json":

        return (
            spark.read
            .format("json")
            .option(
                "inferSchema",
                str(infer_schema_flag).lower()
            )
            .load(file_path)
        )

    elif normalized_format == "parquet":

        return spark.read.parquet(file_path)

    else:

        raise ValueError(
            f"Unsupported file format: {file_format}"
        )

# COMMAND ----------

# ============================================================
# CELL 12: MOVE FILE FUNCTION
# ============================================================

def move_file(source_file_path, destination_folder, file_name):
    """
    Move a source file into archive or error folder.
    """

    dbutils.fs.mkdirs(destination_folder)

    timestamp_text = datetime.now().strftime("%Y%m%d_%H%M%S")

    destination_file_path = (
        f"{destination_folder.rstrip('/')}/"
        f"{timestamp_text}_{file_name}"
    )

    dbutils.fs.mv(
        source_file_path,
        destination_file_path
    )

    return destination_file_path

# COMMAND ----------

# ============================================================
# CELL 13: PROCESS ONE FILE FUNCTION
# ============================================================

def process_one_file(config, file_info):

    audit_id = str(uuid4())
    start_timestamp = datetime.now()

    config_id = config["config_id"]
    source_name = config["source_name"]
    file_format = config["file_format"]
    delimiter_name = config["delimiter_name"]
    header_flag = config["header_flag"]
    infer_schema_flag = config["infer_schema_flag"]
    load_type = config["load_type"].strip().upper()

    source_file_name = file_info.name
    source_file_path = file_info.path

    target_table = (
        f"{config['target_catalog']}."
        f"{config['target_schema']}."
        f"{config['target_table']}"
    )

    # Use complete paths directly from configuration.
    archive_path = config["archive_path"]
    error_path = config["error_path"]

    records_read = 0
    records_written = 0

    print("-" * 80)
    print("Processing file:", source_file_name)
    print("Source path:", source_file_path)
    print("Target table:", target_table)

    try:
        # ----------------------------------------------------
        # 1. Check for duplicate processing
        # ----------------------------------------------------

        if was_file_processed(config_id, source_file_path):

            print("SKIPPED: File was already processed successfully.")

            write_audit_record(
                audit_id=audit_id,
                run_id=RUN_ID,
                config_id=config_id,
                source_name=source_name,
                source_file=source_file_name,
                source_path=source_file_path,
                target_table=target_table,
                load_type=load_type,
                start_timestamp=start_timestamp,
                end_timestamp=datetime.now(),
                records_read=0,
                records_written=0,
                status="SKIPPED",
                error_message="File was already processed successfully."
            )

            return

        # ----------------------------------------------------
        # 2. Convert configured delimiter
        # ----------------------------------------------------

        actual_delimiter = get_delimiter(delimiter_name)

        print("Configured delimiter:", delimiter_name)
        print("Actual delimiter:", repr(actual_delimiter))

        # ----------------------------------------------------
        # 3. Read source file
        # ----------------------------------------------------

        source_df = read_source_file(
            file_path=source_file_path,
            file_format=file_format,
            delimiter=actual_delimiter,
            header_flag=header_flag,
            infer_schema_flag=infer_schema_flag
        )

        records_read = source_df.count()

        print("Records read:", records_read)

        if records_read == 0:
            raise ValueError("The source file is empty.")

        # ----------------------------------------------------
        # 4. Add framework audit columns
        # ----------------------------------------------------

        output_df = (
            source_df
            .withColumn(
                "_source_file_name",
                F.lit(source_file_name)
            )
            .withColumn(
                "_source_file_path",
                F.lit(source_file_path)
            )
            .withColumn(
                "_config_id",
                F.lit(config_id)
            )
            .withColumn(
                "_run_id",
                F.lit(RUN_ID)
            )
            .withColumn(
                "_ingestion_timestamp",
                F.current_timestamp()
            )
        )

        # ----------------------------------------------------
        # 5. Ensure target schema exists
        # ----------------------------------------------------

        spark.sql(
            f"""
            CREATE SCHEMA IF NOT EXISTS
            {config['target_catalog']}.{config['target_schema']}
            """
        )

        # ----------------------------------------------------
        # 6. Load target Delta table
        # ----------------------------------------------------

        if load_type == "APPEND":

            (
                output_df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(target_table)
            )

        elif load_type == "OVERWRITE":

            (
                output_df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(target_table)
            )

        else:

            raise ValueError(
                f"Unsupported load type: {load_type}. "
                "Use APPEND or OVERWRITE."
            )

        records_written = output_df.count()

        print("Records written:", records_written)
        if records_written != records_read:
            print(
                "WARNING: The number of records written does not match "
                "the number of records read."
            )
        else:
            print("Data loaded successfully.")

        # ----------------------------------------------------
        # 7. Write successful audit record
        # ----------------------------------------------------

        write_audit_record(
            audit_id=audit_id,
            run_id=RUN_ID,
            config_id=config_id,
            source_name=source_name,
            source_file=source_file_name,
            source_path=source_file_path,
            target_table=target_table,
            load_type=load_type,
            start_timestamp=start_timestamp,
            end_timestamp=datetime.now(),
            records_read=records_read,
            records_written=records_written,
            status="SUCCESS",
            error_message=None
        )

        print("Data loaded successfully.")

        # ----------------------------------------------------
        # 8. Move file to archive
        # ----------------------------------------------------

        archived_file_path = move_file(
            source_file_path=source_file_path,
            destination_folder=archive_path,
            file_name=source_file_name
        )

        print("File archived successfully:")
        print(archived_file_path)

    except Exception as error:

        error_message = str(error)

        print("FAILED:", error_message)

        # Write failed audit record.
        try:
            write_audit_record(
                audit_id=audit_id,
                run_id=RUN_ID,
                config_id=config_id,
                source_name=source_name,
                source_file=source_file_name,
                source_path=source_file_path,
                target_table=target_table,
                load_type=load_type,
                start_timestamp=start_timestamp,
                end_timestamp=datetime.now(),
                records_read=records_read,
                records_written=records_written,
                status="FAILED",
                error_message=error_message[:4000]
            )

        except Exception as audit_error:
            print(
                "The audit record could not be written:",
                str(audit_error)
            )

        # Move failed file to error folder.
        try:
            failed_file_path = move_file(
                source_file_path=source_file_path,
                destination_folder=error_path,
                file_name=source_file_name
            )

            print("Failed file moved to:")
            print(failed_file_path)

        except Exception as move_error:
            print(
                "The failed file could not be moved:",
                str(move_error)
            )

# COMMAND ----------

def was_file_processed(config_id, source_file):
    """
    Check whether this file was already processed successfully.
    """

    result = spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM {AUDIT_TABLE}
        WHERE config_id = {config_id}
          AND source_file = '{source_file}'
          AND status = 'SUCCESS'
    """).collect()[0]["cnt"]

    return result > 0

# COMMAND ----------

# ============================================================
# MAIN FRAMEWORK EXECUTION
# ============================================================

active_configs = config_df.collect()

if len(active_configs) == 0:

    print("No active configurations found.")

else:

    print(
        f"Starting framework for "
        f"{len(active_configs)} active configuration(s)."
    )

    for config_row in active_configs:

        config = config_row.asDict()

        config_start_timestamp = datetime.now()
        config_audit_id = str(uuid4())

        config_id = config["config_id"]
        source_name = config["source_name"]
        source_path = config["source_path"]
        file_name_pattern = config["file_name_pattern"]

        load_type = (
            config["load_type"].strip().upper()
            if config["load_type"]
            else None
        )

        target_table = (
            f"{config['target_catalog']}."
            f"{config['target_schema']}."
            f"{config['target_table']}"
        )

        print("\n" + "=" * 80)
        print("Configuration ID:", config_id)
        print("Source name:", source_name)
        print("File pattern:", file_name_pattern)
        print("Configured source path:", source_path)

        try:

            # ------------------------------------------------
            # 1. Find files matching the configured pattern
            # ------------------------------------------------

            matching_files = find_matching_files(
                source_path=source_path,
                file_name_pattern=file_name_pattern
            )

            print(
                "Number of matching files:",
                len(matching_files)
            )

            # ------------------------------------------------
            # 2. No files available - write SKIPPED audit
            # ------------------------------------------------

            if len(matching_files) == 0:

                skip_message = (
                    f"No files matched pattern "
                    f"'{file_name_pattern}' inside "
                    f"'{source_path}'."
                )

                print("SKIPPED:", skip_message)

                write_audit_record(
                    audit_id=config_audit_id,
                    run_id=RUN_ID,
                    config_id=config_id,
                    source_name=source_name,

                    # Since there is no actual file, store
                    # the expected filename pattern.
                    source_file=file_name_pattern,

                    source_path=source_path,
                    target_table=target_table,
                    load_type=load_type,
                    start_timestamp=config_start_timestamp,
                    end_timestamp=datetime.now(),
                    records_read=0,
                    records_written=0,
                    status="SKIPPED",
                    error_message=skip_message
                )

                # Continue with the next configuration.
                continue

            # ------------------------------------------------
            # 3. Process every matching file
            # ------------------------------------------------

            for file_info in matching_files:

                process_one_file(
                    config=config,
                    file_info=file_info
                )

        except FileNotFoundError as file_error:

            # ------------------------------------------------
            # 4. Source path/file unavailable - SKIPPED
            # ------------------------------------------------

            skip_message = str(file_error)

            print("SKIPPED:", skip_message)

            try:

                write_audit_record(
                    audit_id=config_audit_id,
                    run_id=RUN_ID,
                    config_id=config_id,
                    source_name=source_name,
                    source_file=file_name_pattern,
                    source_path=source_path,
                    target_table=target_table,
                    load_type=load_type,
                    start_timestamp=config_start_timestamp,
                    end_timestamp=datetime.now(),
                    records_read=0,
                    records_written=0,
                    status="SKIPPED",
                    error_message=skip_message[:4000]
                )

            except Exception as audit_error:

                print(
                    "The SKIPPED audit record could not be written:",
                    str(audit_error)
                )

            # Do not stop the framework.
            # Continue with the next configuration.
            continue

        except Exception as configuration_error:

            # ------------------------------------------------
            # 5. Other configuration errors - FAILED
            # ------------------------------------------------

            error_message = str(configuration_error)

            print(
                "Configuration processing failed:",
                error_message
            )

            try:

                write_audit_record(
                    audit_id=config_audit_id,
                    run_id=RUN_ID,
                    config_id=config_id,
                    source_name=source_name,
                    source_file=file_name_pattern,
                    source_path=source_path,
                    target_table=target_table,
                    load_type=load_type,
                    start_timestamp=config_start_timestamp,
                    end_timestamp=datetime.now(),
                    records_read=0,
                    records_written=0,
                    status="FAILED",
                    error_message=error_message[:4000]
                )

            except Exception as audit_error:

                print(
                    "The FAILED audit record could not be written:",
                    str(audit_error)
                )

            # Continue processing the remaining configurations.
            continue

    print("\n" + "=" * 80)
    print("Framework execution completed.")

# COMMAND ----------

# ============================================================
# CELL 15: DISPLAY CURRENT RUN AUDIT
# ============================================================

current_run_audit_df = (
    spark.table(AUDIT_TABLE)
    .filter(F.col("run_id") == RUN_ID)
    .orderBy("start_timestamp")
)

display(current_run_audit_df)

# COMMAND ----------

# ============================================================
# CELL 16: SHOW TARGET TABLES
# ============================================================

target_locations = (
    config_df
    .select(
        "target_catalog",
        "target_schema"
    )
    .distinct()
    .collect()
)

for location in target_locations:

    target_catalog = location["target_catalog"]
    target_schema = location["target_schema"]

    print(
        f"Tables inside "
        f"{target_catalog}.{target_schema}:"
    )

    display(
        spark.sql(
            f"""
            SHOW TABLES IN
            {target_catalog}.{target_schema}
            """
        )
    )

# COMMAND ----------

# ============================================================
# CELL 17: DISPLAY LOADED TARGET DATA
# ============================================================

target_tables = (
    config_df
    .select(
        "target_catalog",
        "target_schema",
        "target_table"
    )
    .distinct()
    .collect()
)

for table_config in target_tables:

    full_target_table = (
        f"{table_config['target_catalog']}."
        f"{table_config['target_schema']}."
        f"{table_config['target_table']}"
    )

    if spark.catalog.tableExists(full_target_table):

        print("\n" + "=" * 80)
        print("Target table:", full_target_table)

        display(
            spark.table(full_target_table)
        )

    else:

        print(
            f"Table does not exist yet: "
            f"{full_target_table}"
        )
