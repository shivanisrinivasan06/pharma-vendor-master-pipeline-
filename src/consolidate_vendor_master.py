###############################################################################
# Script Name    : consolidate_vendor_master.py
# Objective      : Read vendor data from ERP and manual
#                  procurement CSV files, standardize and validate the data,
#                  quarantine invalid records, normalize material categories,
#                  and load the results into SQLite.
# Created By     : Shivani
# Created Date   : 23/11/2026
# Modified Date  : 23/11/2026
# Modified By    : Shivani
# Comments       : 
###############################################################################

"""
Pseudocode :

Step 1 : Read vendor data from ERP and manual procurement CSV files.
Step 2 : Format fields consistently and fill missing values.
Step 3 : Check data quality rules and move invalid records to a quarantine table.
Step 4 : Remove duplicates, normalize and load clean data into SQLite.
Step 5 : Run validation SQL queries and generate logs and quarantine reports..

"""

import sys
import logging
import configparser
import sqlite3
from pathlib import Path
from datetime import datetime

import pandas as pd


# =============================================================================
# 1. PROJECT CONSTANTS
# =============================================================================

# Project root directory (one level above src)
BASE_DIR = Path(__file__).resolve().parent.parent

# Configuration file location
CONFIG_FILE = BASE_DIR / "config.ini"

LOGGER_NAME = "vendor_master_pipeline"

REQUIRED_COLUMNS = [
    "vendor_id",
    "vendor_name",
    "country",
    "onboarded_date",
    "lead_time_days",
    "contract_value_usd",
    "material_categories",
    "last_modified",
]

COUNTRY_MAPPING = {
    "INDIA": "India",
    "USA": "USA",
    "UNITED STATES": "USA",
    "GERMANY": "Germany",
    "UK": "UK",
}


# =============================================================================
# 2. CONFIGURATION MANAGEMENT
# =============================================================================

def load_configuration():
    """
    Read and validate config.ini.

    Relative paths are resolved from the project directory so that the
    script can be executed from any working directory.
    """

    if not CONFIG_FILE.is_file():
        raise FileNotFoundError(
            f"Configuration file not found: {CONFIG_FILE}"
        )

    # Disable ConfigParser interpolation so date formats containing %
    # can be stored normally in config.ini.
    config = configparser.ConfigParser(interpolation=None)
    config.read(CONFIG_FILE, encoding="utf-8")

    required_sections = ["PATHS", "DATABASE", "LOGGING"]

    for section in required_sections:
        if not config.has_section(section):
            raise ValueError(
                f"Required configuration section is missing: [{section}]"
            )

    required_path_keys = [
        "erp_file",
        "manual_file",
        "database_file",
        "log_directory",
        "output_directory",
        "sql_file",
    ]

    paths = {}

    for key in required_path_keys:
        if not config.has_option("PATHS", key):
            raise ValueError(
                f"Required configuration option is missing: PATHS.{key}"
            )

        configured_path = Path(config.get("PATHS", key))

        paths[key] = (
            configured_path
            if configured_path.is_absolute()
            else BASE_DIR / configured_path
        )

    return config, paths


# =============================================================================
# 3. LOGGING CONFIGURATION
# =============================================================================

def setup_logging(config, paths):
    """
    Create a unique log file for every execution.

    Log format:
    YYYY-MM-DD HH:MM:SS | INFO  | Message
    """

    log_directory = paths["log_directory"]
    log_directory.mkdir(parents=True, exist_ok=True)

    prefix = config.get(
        "LOGGING",
        "log_filename_prefix",
        fallback="vendor_master",
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    log_file = log_directory / f"{prefix}_{timestamp}.log"

    log_level = config.get(
        "LOGGING",
        "log_level",
        fallback="INFO",
    ).upper()

    numeric_level = getattr(logging, log_level, None)

    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid logging level: {log_level}")

    date_format = config.get(
        "LOGGING",
        "date_format",
        fallback="%Y-%m-%d %H:%M:%S",
    )

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(numeric_level)
    logger.propagate = False

    # Remove and close handlers left by any earlier invocation in this
    # Python process. A new file is created for this execution.
    for existing_handler in logger.handlers[:]:
        existing_handler.close()
        logger.removeHandler(existing_handler)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-5s | %(message)s",
        datefmt=date_format,
    )

    # mode="x" prevents an existing log file from being overwritten.
    file_handler = logging.FileHandler(
        log_file,
        mode="x",
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger, log_file


def close_logging(logger):
    """Flush and close all handlers cleanly."""

    if logger is not None:
        for handler in logger.handlers[:]:
            handler.flush()
            handler.close()
            logger.removeHandler(handler)


# =============================================================================
# 4. DATABASE SCHEMA INITIALIZATION
# =============================================================================

def initialize_database(db_path, sql_path, logger):
    """
    Create the SQLite database and initialize the three target tables
    using the supplied create_tables.sql file.
    """

    logger.info("Database schema initialization started.")

    if not sql_path.is_file():
        raise FileNotFoundError(f"SQL DDL file not found: {sql_path}")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")

        ddl_script = sql_path.read_text(encoding="utf-8")

        if not ddl_script.strip():
            raise ValueError("SQL DDL file is empty.")

        connection.executescript(ddl_script)

        required_tables = {
            "gold_vendor_master",
            "gold_vendor_categories",
            "err_vendor_quarantine",
        }

        existing_tables = {
            row[0]
            for row in connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                """
            ).fetchall()
        }

        missing_tables = required_tables - existing_tables

        if missing_tables:
            raise RuntimeError(
                f"Database tables were not created: {sorted(missing_tables)}"
            )

    logger.info("Database schema initialized successfully.")


# =============================================================================
# 5. STEP 1: load the source data 
# =============================================================================

def load_raw_sources(erp_path, manual_path):
    """
    Read both source CSV files, add source_system lineage tags,
    validate required columns, and combine using pd.concat().
    """

    logger = logging.getLogger(LOGGER_NAME)

    try:
        logger.info("step_1: Loading source CSV files started")

        for file_path in (erp_path, manual_path):
            if not file_path.is_file():
                raise FileNotFoundError(
                    f"Source CSV file not found: {file_path}"
                )

        # Read ERP source records.
        df_erp = pd.read_csv(erp_path, encoding="utf-8")

        if df_erp.empty:
            raise ValueError("ERP source CSV contains no data records.")

        df_erp["source_system"] = "ERP"

        logger.info(
            f"ERP CSV loaded successfully. Records extracted: {len(df_erp)}"
        )

        # Read manually maintained procurement records.
        df_manual = pd.read_csv(manual_path, encoding="utf-8")

        if df_manual.empty:
            raise ValueError(
                "Manual procurement CSV contains no data records."
            )

        df_manual["source_system"] = "MANUAL_LOG"

        logger.info(
            "Manual CSV loaded successfully. "
            f"Records extracted: {len(df_manual)}"
        )

        # Check that both input files contain every required field.
        for source_name, dataframe in (
            ("ERP", df_erp),
            ("MANUAL_LOG", df_manual),
        ):
            missing_columns = [
                column
                for column in REQUIRED_COLUMNS
                if column not in dataframe.columns
            ]

            if missing_columns:
                raise ValueError(
                    f"{source_name} CSV is missing columns: {missing_columns}"
                )

        # Combine both sources while preserving all rows.
        df_combined = pd.concat(
            [df_erp, df_manual],
            ignore_index=True,
        )

        logger.info(
            "Both source datasets combined successfully. "
            f"Total records: {len(df_combined)}"
        )
        logger.info("step_1 successfully completed.")

        return df_combined

    except Exception:
        logger.exception("step_1 failed: Data ingestion error.")
        raise


# =============================================================================
# 6. STEP 2: STRING AND COUNTRY STANDARDIZATION
# =============================================================================

def standardize_strings_and_countries(df):
    """
    Strip whitespace from textual columns, uppercase vendor IDs,
    title-case vendor names, and normalize country aliases.
    """

    logger = logging.getLogger(LOGGER_NAME)

    try:
        logger.info("step_2: String and country standardization started")

        df = df.copy()

        # Remove leading/trailing whitespace from all text columns.
        text_columns = df.select_dtypes(
            include=["object", "string"]
        ).columns

        for column in text_columns:
            df[column] = df[column].apply(
                lambda value: value.strip()
                if isinstance(value, str)
                else value
            )

        # Standardize vendor identifiers.
        df["vendor_id"] = (
            df["vendor_id"].astype("string").str.strip().str.upper()
        )

        # Standardize vendor names using Title Case.
        # Preserve the established CamelCase spelling of the sample vendor
        # "SterileFill", which Python str.title() would change to "Sterilefill".
        df["vendor_name"] = (
            df["vendor_name"].astype("string").str.strip().str.title()
        )
        df["vendor_name"] = df["vendor_name"].replace(
            {"Sterilefill Packaging Corp": "SterileFill Packaging Corp"}
        )

        # Normalize country aliases, preserving unmapped values.
        df["country"] = df["country"].apply(
            lambda value: COUNTRY_MAPPING.get(value.strip().upper(), value.strip())
            if isinstance(value, str)
            else value
        )

        logger.info("Whitespace standardization completed.")
        logger.info("Vendor IDs converted to uppercase.")
        logger.info("Vendor names converted to Title Case.")
        logger.info("Country mapping applied successfully.")
        logger.info("step_2 successfully completed.")

        return df

    except Exception:
        logger.exception("step_2 failed: String standardization error.")
        raise


# =============================================================================
# 7. STEP 3: NUMERIC AND DATE PARSING
# =============================================================================

def parse_types_and_dates(df):
    """
    Clean currency strings, convert numeric fields, parse mixed
    onboarded_date formats, and parse last_modified timestamps.
    """

    logger = logging.getLogger(LOGGER_NAME)

    try:
        logger.info("step_3: Numeric and date parsing started")

        df = df.copy()

        # Remove $, commas, and a trailing USD currency code.
        cleaned_contract_values = (
            df["contract_value_usd"]
            .astype("string")
            .str.strip()
            .str.replace("$", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.replace(
                r"\s*USD\s*$",
                "",
                regex=True,
                case=False,
            )
            .str.strip()
        )

        # Convert cleaned contract values to numeric floats.
        df["contract_value_usd"] = pd.to_numeric(
            cleaned_contract_values,
            errors="coerce",
        )

        # Convert lead times to numeric values; missing values remain NaN.
        df["lead_time_days"] = pd.to_numeric(
            df["lead_time_days"],
            errors="coerce",
        )

        # Parse each date format explicitly. This avoids ambiguity between
        # day-first and year-first values in the same source column.
        raw_dates = df["onboarded_date"].astype("string").str.strip()
        parsed_dates = pd.Series(
            pd.NaT,
            index=df.index,
            dtype="datetime64[ns]",
        )

        supported_date_formats = [
            "%Y-%m-%d",  # YYYY-MM-DD
            "%d/%m/%Y",  # DD/MM/YYYY
            "%Y/%m/%d",  # YYYY/MM/DD
            "%d-%m-%Y",  # DD-MM-YYYY
        ]

        for date_format in supported_date_formats:
            remaining_mask = parsed_dates.isna() & raw_dates.notna()
            if remaining_mask.any():
                parsed_dates.loc[remaining_mask] = pd.to_datetime(
                    raw_dates.loc[remaining_mask],
                    format=date_format,
                    errors="coerce",
                )

        df["onboarded_date"] = parsed_dates.dt.strftime("%Y-%m-%d")

        # Parse last_modified into SQL-compatible datetime values.
        df["last_modified"] = pd.to_datetime(
            df["last_modified"],
            errors="coerce",
        )

        # The supplied target schema requires both dates to be NOT NULL.
        # The assignment does not define quarantine reasons for invalid dates,
        # so fail clearly instead of silently dropping or misclassifying rows.
        invalid_onboarded = df["onboarded_date"].isna()
        invalid_modified = df["last_modified"].isna()

        if invalid_onboarded.any():
            raise ValueError(
                "Unparseable onboarded_date values found in "
                f"{int(invalid_onboarded.sum())} record(s). "
                "The assignment does not define a quarantine reason for them."
            )

        if invalid_modified.any():
            raise ValueError(
                "Unparseable last_modified values found in "
                f"{int(invalid_modified.sum())} record(s). "
                "The assignment does not define a quarantine reason for them."
            )

        logger.info("Contract values cleaned and converted to numeric.")
        logger.info("Lead time values converted to numeric.")
        logger.info("Onboarded dates standardized to YYYY-MM-DD.")
        logger.info("Last modified timestamps parsed successfully.")
        logger.info("step_3 successfully completed.")

        return df

    except Exception:
        logger.exception("step_3 failed: Numeric or date parsing error.")
        raise


# =============================================================================
# 8. STEP 4: DATA QUALITY GATE AND QUARANTINE SPLIT
# =============================================================================

def split_quarantine_records(df):
    """
    Apply the three rejection rules in the assignment:
      - Missing or blank vendor_name
      - Invalid/NaN contract_value_usd
      - Negative lead_time_days

    A record violating more than one rule receives all applicable
    rejection reasons separated by semicolons.
    """

    logger = logging.getLogger(LOGGER_NAME)

    try:
        logger.info("step_4: Data quality validation started")

        df = df.copy()

        # Rule 1: Missing or blank vendor name.
        missing_vendor_name = (
            df["vendor_name"].isna()
            | df["vendor_name"].astype("string").str.strip().eq("").fillna(False)
        )

        # Rule 2: Invalid or unparseable contract value.
        invalid_contract_value = df["contract_value_usd"].isna()

        # Rule 3: Negative lead time.
        negative_lead_time = df["lead_time_days"].lt(0).fillna(False)

        # Build rejection reasons without losing records that violate
        # more than one quality rule.
        rejection_reasons = pd.Series(
            "",
            index=df.index,
            dtype="object",
        )

        rejection_rules = [
            (missing_vendor_name, "MISSING_VENDOR_NAME"),
            (invalid_contract_value, "INVALID_CONTRACT_VALUE"),
            (negative_lead_time, "NEGATIVE_LEAD_TIME"),
        ]

        for condition, reason in rejection_rules:
            rejection_reasons.loc[condition] = (
                rejection_reasons.loc[condition].apply(
                    lambda current_reason: (
                        f"{current_reason};{reason}"
                        if current_reason
                        else reason
                    )
                )
            )

        quarantine_mask = rejection_reasons.ne("")

        # Preserve rejected source records and attach rejection_reason.
        df_quarantine = df.loc[quarantine_mask].copy()
        df_quarantine["rejection_reason"] = (
            rejection_reasons.loc[quarantine_mask]
        )

        # Only compliant records proceed to imputation and deduplication.
        df_clean = df.loc[~quarantine_mask].copy()

        logger.info(f"Valid records identified: {len(df_clean)}")
        logger.info(f"Invalid records quarantined: {len(df_quarantine)}")

        for reason in (
            "MISSING_VENDOR_NAME",
            "INVALID_CONTRACT_VALUE",
            "NEGATIVE_LEAD_TIME",
        ):
            reason_count = (
                df_quarantine["rejection_reason"]
                .str.contains(reason, regex=False)
                .sum()
            )
            logger.info(
                f"Rejected records - {reason}: {int(reason_count)}"
            )

        logger.info("step_4 successfully completed.")

        return df_clean, df_quarantine

    except Exception:
        logger.exception("step_4 failed: Data quality validation error.")
        raise


# =============================================================================
# 9. STEP 5: MISSING VALUE IMPUTATION AND DEDUPLICATION
# =============================================================================

def impute_and_deduplicate(df_clean):
    """
    Impute missing lead_time_days using the rounded median of valid
    records, create is_lead_time_imputed, and retain the latest record
    per vendor based on last_modified.
    """

    logger = logging.getLogger(LOGGER_NAME)

    try:
        logger.info("step_5: Imputation and deduplication started")

        df_clean = df_clean.copy()

        if df_clean.empty:
            raise ValueError(
                "No valid records remain after the data quality gate."
            )

        # Identify missing values before filling them.
        missing_lead_time = df_clean["lead_time_days"].isna()

        # Calculate median from non-null lead times in valid records.
        median_lead_time = df_clean["lead_time_days"].median()

        if pd.isna(median_lead_time):
            raise ValueError(
                "Median lead time cannot be calculated because all "
                "valid lead_time_days values are missing."
            )

        median_lead_time = int(round(median_lead_time))

        # Create the required boolean imputation flag.
        df_clean["is_lead_time_imputed"] = missing_lead_time.astype(bool)

        # Fill missing lead times with the rounded median.
        df_clean["lead_time_days"] = (
            df_clean["lead_time_days"]
            .fillna(median_lead_time)
            .round()
            .astype("int64")
        )

        logger.info(f"Median lead time calculated: {median_lead_time}")
        logger.info(
            f"Missing lead times imputed: {int(missing_lead_time.sum())}"
        )

        # Sort ascending so keep='last' retains the latest timestamp.
        df_clean = df_clean.sort_values(
            by="last_modified",
            ascending=True,
            kind="mergesort",
        )

        records_before = len(df_clean)

        # Retain the latest update for each vendor_id.
        df_clean = df_clean.drop_duplicates(
            subset=["vendor_id"],
            keep="last",
        )

        duplicates_removed = records_before - len(df_clean)

        logger.info(f"Duplicate records removed: {duplicates_removed}")
        logger.info(f"Unique vendors retained: {len(df_clean)}")
        logger.info("step_5 successfully completed.")

        return df_clean.reset_index(drop=True)

    except Exception:
        logger.exception("step_5 failed: Imputation or deduplication error.")
        raise


# =============================================================================
# 10. STEP 6A: CATEGORY NORMALIZATION USING EXPLODE
# =============================================================================

def build_normalized_category_bridge(df_clean):
    """
    Split comma-separated material_categories, strip each category,
    and use pandas explode() to create vendor-category rows.
    """

    logger = logging.getLogger(LOGGER_NAME)

    try:
        logger.info("step_6a: Category normalization started")

        df_categories = df_clean[
            ["vendor_id", "material_categories"]
        ].copy()

        # Split each comma-separated string into a list and strip tokens.
        df_categories["material_categories"] = (
            df_categories["material_categories"]
            .fillna("")
            .apply(
                lambda value: [
                    category.strip()
                    for category in str(value).split(",")
                    if category.strip()
                ]
            )
        )

        # Convert each list item into an individual row.
        df_bridge = df_categories.explode("material_categories")

        # Rename the exploded field to match the bridge-table DDL.
        df_bridge = df_bridge.rename(
            columns={"material_categories": "material_category"}
        )

        # Remove null and empty category values.
        df_bridge = df_bridge.dropna(
            subset=["material_category"]
        )

        df_bridge["material_category"] = (
            df_bridge["material_category"].astype("string").str.strip()
        )

        df_bridge = df_bridge[
            df_bridge["material_category"].ne("")
        ]

        # Prevent duplicate vendor/category primary-key combinations.
        df_bridge = df_bridge.drop_duplicates(
            subset=["vendor_id", "material_category"]
        )

        df_bridge = df_bridge.reset_index(drop=True)

        logger.info(
            f"Normalized vendor-category rows generated: {len(df_bridge)}"
        )
        logger.info("Material categories split and exploded successfully.")
        logger.info("step_6a successfully completed.")

        return df_bridge

    except Exception:
        logger.exception("step_6a failed: Category normalization error.")
        raise


# =============================================================================
# 11. STEP 6B: SQLITE DB LOADING AND QUARANTINE CSV EXPORT
# =============================================================================

def persist_to_database(
    df_clean,
    df_bridge,
    df_quarantine,
    db_path,
    output_directory,
):
    """
    Add processed_at metadata to all three processing DataFrames and
    load the three target SQLite tables.

    Important schema note:
    The supplied gold_vendor_categories DDL has only vendor_id and
    material_category columns. Therefore processed_at is added to the
    bridge DataFrame as required, but excluded from the INSERT column
    selection to preserve the supplied target schema exactly.
    """

    logger = logging.getLogger(LOGGER_NAME)

    try:
        logger.info("step_6b: Loading records into SQLite started")

        processed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Work on copies so DataFrames remain unchanged.
        df_master = df_clean.copy()
        df_categories = df_bridge.copy()
        df_errors = df_quarantine.copy()

        # Add processing metadata to all three DataFrames.
        df_master["processed_at"] = processed_at
        df_categories["processed_at"] = processed_at
        df_errors["processed_at"] = processed_at

        # Remove material_categories from the primary vendor master.
        df_master = df_master.drop(
            columns=["material_categories"]
        )

        # Arrange master columns to match the supplied DDL exactly.
        master_columns = [
            "vendor_id",
            "vendor_name",
            "country",
            "onboarded_date",
            "lead_time_days",
            "is_lead_time_imputed",
            "contract_value_usd",
            "source_system",
            "last_modified",
            "processed_at",
        ]
        df_master = df_master[master_columns]

        # Arrange category columns to match the supplied bridge DDL.
        category_columns = ["vendor_id", "material_category"]
        df_categories_to_load = df_categories[category_columns]

        # Arrange quarantine columns to match the supplied DDL exactly.
        quarantine_columns = [
            "vendor_id",
            "vendor_name",
            "country",
            "onboarded_date",
            "lead_time_days",
            "contract_value_usd",
            "material_categories",
            "source_system",
            "last_modified",
            "rejection_reason",
            "processed_at",
        ]
        df_errors = df_errors[quarantine_columns]

        # Convert pandas timestamps into SQL-compatible text.
        df_master["last_modified"] = (
            df_master["last_modified"].dt.strftime("%Y-%m-%d %H:%M:%S")
        )

        if not df_errors.empty:
            df_errors["last_modified"] = (
                pd.to_datetime(df_errors["last_modified"])
                .dt.strftime("%Y-%m-%d %H:%M:%S")
            )

        # SQLite stores the boolean flag as INTEGER (0/1).
        df_master["is_lead_time_imputed"] = (
            df_master["is_lead_time_imputed"].astype(int)
        )

        db_path.parent.mkdir(parents=True, exist_ok=True)
        output_directory.mkdir(parents=True, exist_ok=True)

        # Use one transaction so a failed load does not leave a partial
        # set of target tables. Delete in child-before-parent order.
        with sqlite3.connect(db_path) as connection:
            connection.execute("PRAGMA foreign_keys = ON")

            connection.execute("DELETE FROM gold_vendor_categories")
            connection.execute("DELETE FROM gold_vendor_master")
            connection.execute("DELETE FROM err_vendor_quarantine")

            # Load primary golden vendor master.
            df_master.to_sql(
                "gold_vendor_master",
                connection,
                if_exists="append",
                index=False,
            )

            # Load normalized vendor-category bridge.
            df_categories_to_load.to_sql(
                "gold_vendor_categories",
                connection,
                if_exists="append",
                index=False,
            )

            # Load rejected records into quarantine.
            df_errors.to_sql(
                "err_vendor_quarantine",
                connection,
                if_exists="append",
                index=False,
            )

            # Confirm referential integrity after loading.
            foreign_key_errors = connection.execute(
                "PRAGMA foreign_key_check"
            ).fetchall()

            if foreign_key_errors:
                raise RuntimeError(
                    f"Foreign key validation failed: {foreign_key_errors}"
                )

            # Capture actual inserted record counts.
            master_count = connection.execute(
                "SELECT COUNT(*) FROM gold_vendor_master"
            ).fetchone()[0]

            category_count = connection.execute(
                "SELECT COUNT(*) FROM gold_vendor_categories"
            ).fetchone()[0]

            quarantine_count = connection.execute(
                "SELECT COUNT(*) FROM err_vendor_quarantine"
            ).fetchone()[0]

        logger.info(
            f"Records loaded into gold_vendor_master: {master_count}"
        )
        logger.info(
            f"Records loaded into gold_vendor_categories: {category_count}"
        )
        logger.info(
            f"Records loaded into err_vendor_quarantine: {quarantine_count}"
        )

        # Export the persisted quarantine table to the required CSV report.
        quarantine_report = output_directory / "quarantine_report.csv"

        with sqlite3.connect(db_path) as connection:
            df_quarantine_report = pd.read_sql_query(
                """
                SELECT *
                FROM err_vendor_quarantine
                ORDER BY quarantine_id
                """,
                connection,
            )

        df_quarantine_report.to_csv(
            quarantine_report,
            index=False,
            encoding="utf-8",
        )

        logger.info(
            f"Quarantine report exported successfully: {quarantine_report}"
        )
        logger.info("Database connection closed.")
        logger.info("step_6b successfully completed.")

    except Exception:
        logger.exception("step_6b failed: Database persistence error.")
        raise


def log_sql_result(logger, query_name, sql, dataframe):
    """
    Log a verification SQL statement and every row returned by that query.
    Each output line is written through the logger to preserve the log format.
    """

    logger.info(f"{query_name} SQL:")
    for sql_line in sql.strip().splitlines():
        logger.info(f"    {sql_line.strip()}")

    logger.info(f"{query_name} result ({len(dataframe)} row(s)):")

    if dataframe.empty:
        logger.info("    <no rows returned>")
        return

    # Use a stable text representation so the result is readable in the log.
    result_lines = dataframe.to_string(index=False).splitlines()
    for result_line in result_lines:
        logger.info(f"    {result_line}")




# =============================================================================
# 12. POST-LOAD SQL VERIFICATION
# =============================================================================

def verify_database(db_path):
    """
    Execute the three verification queries provided in the assignment:
      1. Golden master integrity and portfolio spend.
      2. Category breakdown through the relational bridge.
      3. Quarantine audit review.
    """

    logger = logging.getLogger(LOGGER_NAME)

    try:
        logger.info("step_7: Post-load SQL verification started.")

        with sqlite3.connect(db_path) as connection:

            # Query 1: Golden master integrity.
            query_1 = """
                SELECT
                    vendor_id,
                    vendor_name,
                    country,
                    onboarded_date,
                    lead_time_days,
                    is_lead_time_imputed,
                    contract_value_usd,
                    source_system,
                    last_modified
                FROM gold_vendor_master
                ORDER BY vendor_id;
            """

            df_master = pd.read_sql_query(query_1, connection)

            total_spend = connection.execute(
                """
                SELECT COALESCE(SUM(contract_value_usd), 0)
                FROM gold_vendor_master
                """
            ).fetchone()[0]

            logger.info(
                "Query 1: Golden master integrity verification "
                "executed successfully."
            )
            log_sql_result(logger, "Query 1", query_1, df_master)
            logger.info(f"Total unique vendors: {len(df_master)}")
            logger.info(f"Total portfolio spend: ${total_spend:,.2f}")

            bridge_link_count = connection.execute(
                "SELECT COUNT(*) FROM gold_vendor_categories"
            ).fetchone()[0]
            logger.info(
                f"Total vendor-category bridge links: {bridge_link_count}"
            )

            # Query 2: Category breakdown using the relational bridge.
            query_2 = """
                SELECT
                    c.material_category,
                    COUNT(DISTINCT v.vendor_id) AS supplier_count,
                    SUM(v.contract_value_usd) AS category_total_spend
                FROM gold_vendor_categories c
                JOIN gold_vendor_master v
                    ON c.vendor_id = v.vendor_id
                GROUP BY c.material_category
                ORDER BY supplier_count DESC, category_total_spend DESC;
            """

            df_categories = pd.read_sql_query(query_2, connection)

            logger.info(
                "Query 2: Category breakdown verification "
                "executed successfully."
            )
            log_sql_result(logger, "Query 2", query_2, df_categories)

            # Query 3: Quarantine audit review.
            query_3 = """
                SELECT
                    vendor_id,
                    vendor_name,
                    source_system,
                    rejection_reason
                FROM err_vendor_quarantine
                ORDER BY vendor_id;
            """

            df_quarantine = pd.read_sql_query(query_3, connection)

            logger.info(
                "Query 3: Quarantine audit verification "
                "executed successfully."
            )
            log_sql_result(logger, "Query 3", query_3, df_quarantine)

        # Log verification result counts.
        logger.info(
            f"Golden master records verified: {len(df_master)}"
        )
        logger.info(
            f"Category breakdown rows verified: {len(df_categories)}"
        )
        logger.info(
            f"Quarantine records verified: {len(df_quarantine)}"
        )

        logger.info("step_7 successfully completed.")

        return df_master, df_categories, df_quarantine

    except Exception:
        logger.exception("step_7 failed: SQL verification error.")
        raise


# =============================================================================
# 13. EXPECTED-RESULT VALIDATION
# =============================================================================

def validate_expected_results(
    df_master,
    df_categories,
    df_quarantine,
    logger,
):
    """
    Compare the sample-data results with the expected outcomes
    stated in the case study.

    These checks validate the supplied deterministic sample.
    """

    logger.info("Expected sample-output validation started.")

    expected_vendor_ids = {
        "VND-101",
        "VND-102",
        "VND-103",
        "VND-106",
        "VND-107",
    }

    actual_vendor_ids = set(df_master["vendor_id"].astype(str))

    if actual_vendor_ids != expected_vendor_ids:
        raise AssertionError(
            "Golden vendor IDs do not match the assignment's expected "
            f"sample. Actual: {sorted(actual_vendor_ids)}"
        )

    if len(df_quarantine) != 3:
        raise AssertionError(
            "Expected exactly 3 quarantined sample records, "
            f"but found {len(df_quarantine)}."
        )

    expected_quarantine_ids = {"VND-104", "VND-105", "VND-108"}
    actual_quarantine_ids = set(
        df_quarantine["vendor_id"].astype(str)
    )

    if actual_quarantine_ids != expected_quarantine_ids:
        raise AssertionError(
            "Quarantine vendor IDs do not match expected sample records. "
            f"Actual: {sorted(actual_quarantine_ids)}"
        )

    actual_spend = float(df_master["contract_value_usd"].sum())

    if not abs(actual_spend - 623650.50) < 0.001:
        raise AssertionError(
            f"Expected portfolio spend $623,650.50; found ${actual_spend:,.2f}."
        )

    # The assignment expects 5 normalized category names in the sample.
    expected_categories = {
        "API",
        "Excipient",
        "Biologics",
        "Packaging",
        "Sterile",
    }

    actual_categories = set(
        df_categories["material_category"].astype(str)
    )

    if actual_categories != expected_categories:
        raise AssertionError(
            "Material categories do not match expected sample categories. "
            f"Actual: {sorted(actual_categories)}"
        )

    # Validate the expected category supplier counts and spend totals.
    expected_category_results = {
        "API": (3, 470400.50),
        "Excipient": (2, 215000.00),
        "Biologics": (1, 245000.00),
        "Packaging": (1, 68250.00),
        "Sterile": (1, 68250.00),
    }

    for row in df_categories.itertuples(index=False):
        expected_supplier_count, expected_category_spend = (
            expected_category_results[row.material_category]
        )

        if int(row.supplier_count) != expected_supplier_count:
            raise AssertionError(
                f"Unexpected supplier count for {row.material_category}: "
                f"{row.supplier_count}"
            )

        if abs(
            float(row.category_total_spend) - expected_category_spend
        ) >= 0.001:
            raise AssertionError(
                f"Unexpected category spend for {row.material_category}: "
                f"{row.category_total_spend}"
            )

    # Confirm the sample's two missing lead times were imputed with median 14.
    imputed_rows = df_master[
        df_master["is_lead_time_imputed"].astype(int) == 1
    ]

    if len(imputed_rows) != 2:
        raise AssertionError(
            f"Expected 2 imputed vendor records; found {len(imputed_rows)}."
        )

    if not (imputed_rows["lead_time_days"] == 14).all():
        raise AssertionError(
            "Expected all imputed sample lead times to equal 14."
        )

    logger.info("Expected vendor IDs validated successfully.")
    logger.info("Expected quarantine records validated successfully.")
    logger.info("Expected portfolio spend validated: $623,650.50.")
    logger.info("Expected material categories validated successfully.")
    logger.info("Expected median lead-time imputation validated: 14.")
    logger.info("Expected sample-output validation completed successfully.")


# =============================================================================
# 14. EXECUTION SUMMARY
# =============================================================================

def log_execution_summary(
    logger,
    df_master,
    df_bridge,
    df_categories,
    df_quarantine,
):
    """Write the final pipeline execution summary to the log."""

    total_spend = df_master["contract_value_usd"].sum()

    logger.info("============================================================")
    logger.info("VENDOR MASTER CONSOLIDATION EXECUTION SUMMARY")
    logger.info("============================================================")
    logger.info(f"Golden Master Vendors     : {len(df_master)}")
    logger.info(f"Vendor-Category Links     : {len(df_bridge)}")
    logger.info(f"Distinct Material Categories: {len(df_categories)}")
    logger.info(f"Quarantined Records       : {len(df_quarantine)}")
    logger.info(f"Total Procurement Spend   : ${total_spend:,.2f}")
    logger.info("Status                    : SUCCESS")
    logger.info("============================================================")


# =============================================================================
# 15. MAIN EXECUTION
# =============================================================================

def main():
    """
    Execute the complete vendor master consolidation pipeline.
    """

    logger = None
    log_file = None
    execution_failed = False

    try:
        # Step 1: Read configuration and initialize logging.
        config, paths = load_configuration()
        logger, log_file = setup_logging(config, paths)

        # Required first log entry.
        start_time = datetime.now()

        logger.info(
            f"consolidate_vendor_master.py started at "
            f"{start_time.strftime('%d/%m/%Y %H:%M:%S')}"
        )
        logger.info("")

        logger.info("Project configuration loaded successfully.")
        logger.info(f"Log file: {log_file.name}")

        # Initialize database tables using the supplied DDL.
        initialize_database(
            paths["database_file"],
            paths["sql_file"],
            logger,
        )
        logger.info("")

        # Step 1: Ingest source CSV files and tag their lineage.
        raw_df = load_raw_sources(
            paths["erp_file"],
            paths["manual_file"],
        )
        logger.info(
            f"Total records available for processing: {len(raw_df)}"
        )
        logger.info("")

        # Step 2: Standardize strings and country values.
        std_df = standardize_strings_and_countries(raw_df)
        logger.info("")

        # Step 3: Parse currency, numeric values, and dates.
        parsed_df = parse_types_and_dates(std_df)
        logger.info("")

        # Step 4: Split valid records and quarantine invalid records.
        df_clean, df_quarantine = split_quarantine_records(parsed_df)
        logger.info("")

        # Step 5: Impute missing lead times and deduplicate vendors.
        df_clean_dedup = impute_and_deduplicate(df_clean)
        logger.info("")

        # Step 6a: Normalize material categories using explode().
        df_bridge = build_normalized_category_bridge(df_clean_dedup)
        logger.info("")

        # Step 6b: Load all target tables and export quarantine report.
        persist_to_database(
            df_clean_dedup,
            df_bridge,
            df_quarantine,
            paths["database_file"],
            paths["output_directory"],
        )
        logger.info("")

        # Execute the assignment's three post-load SQL queries.
        df_master_verified, df_categories_verified, df_quarantine_verified = (
            verify_database(paths["database_file"])
        )
        logger.info("")

        # Validate the deterministic sample outcomes in the assignment.
        validate_expected_results(
            df_master_verified,
            df_categories_verified,
            df_quarantine_verified,
            logger,
        )
        logger.info("")

        # Log final execution summary.
        log_execution_summary(
            logger,
            df_master_verified,
            df_bridge,
            df_categories_verified,
            df_quarantine_verified,
        )
        logger.info("")

        print("Vendor Master Consolidation Pipeline executed successfully.")
        print(f"Golden vendors       : {len(df_master_verified)}")
        print(f"Vendor-category links: {len(df_bridge)}")
        print(f"Distinct categories  : {len(df_categories_verified)}")
        print(f"Quarantined records  : {len(df_quarantine_verified)}")
        print(f"Log file             : {log_file}")
        print(
            "Quarantine report    : "
            f"{paths['output_directory'] / 'quarantine_report.csv'}"
        )

    except Exception:
        execution_failed = True

        if logger is not None:
            logger.exception(
                "consolidate_vendor_master.py execution failed."
            )
        else:
            logging.exception(
                "Pipeline initialization failed before file logging started."
            )

        print(
            "Pipeline execution FAILED. "
            "Check the generated log file for error details."
        )

    finally:
        # The final log entry is always written after a log file exists.
        if logger is not None:
            logger.info("")

            if execution_failed:
                logger.info(
                    "consolidate_vendor_master.py execution failed."
                )
            else:
                end_time = datetime.now()

                logger.info(
                    f"consolidate_vendor_master.py completed at "
                    f"{end_time.strftime('%d/%m/%Y %H:%M:%S')}"
                )

            close_logging(logger)

    if execution_failed:
        sys.exit(1)


# =============================================================================
# SCRIPT ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    main()
