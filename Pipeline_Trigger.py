"""NetSuite CSV-to-SQL Server ETL pipeline.

The script performs the following stages in sequence:

1. Creates SQL Server connectivity and pipeline audit records.
2. Loads configured CSV files into the raw schema.
3. Unpivots monthly FX-rate columns into row-based records.
4. Adds ingestion metadata and validates raw row counts.
5. Checks staging primary keys, relationships, and business rules.
6. Enforces the data-quality gate.
7. Loads SCD history tables and current-state DWH tables.

"""

# %%
# ==============================================================================
# IMPORTS
# ==============================================================================

from datetime import datetime, timezone
from pathlib import Path
import re
from urllib.parse import quote_plus
from uuid import uuid4

import polars as pl
from sqlalchemy import Engine, create_engine, text

# %%
# ==============================================================================
# DATABASE CONNECTION
# ==============================================================================
# Build a trusted local SQL Server connection used by all ETL stages.

connection_string = quote_plus(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=NetSuiteDW;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={connection_string}"
)

# %%
# ==============================================================================
# PIPELINE CONFIGURATION
# ==============================================================================
# Directory containing source CSV files and the raw destination for each file.

DIRECTORY_PATH = Path(
    "D:/Nazeer Joseph Data Universe/Data Load/Netsuite ETL Full Project/data"
)

RAW_TABLES = {
    "customer": "raw.customer",
    "item_category": "raw.item_category",
    "item_pattern": "raw.item_pattern",
    "item": "raw.item",
    "subsidiary": "raw.subsidiary",
    "sales_budget": "raw.sales_budget",
    "transactions": "raw.transactions",
    "transactionline": "raw.transactionline",
    "fx_avg_rate": "raw.fx_avg_rate",
    "deleted_records": "raw.deleted_records",
    "user_rls": "raw.user_rls",
}

# Expected logical keys for staging-view validation. Composite keys contain
# more than one column.

PRIMARY_KEYS = {
    "customer": ["customer_nsid"],
    "deleted_records": ["transaction_nsid"],
    "fx_avg_rate": [
        "original_currency",
        "target_currency",
        "rate_date",
    ],
    "item": ["item_nsid"],
    "item_category": ["item_category_nsid"],
    "item_pattern": ["item_pattern_nsid"],
    "sales_budget": ["sales_budget_id"],
    "subsidiary": ["bu_nsid"],
    "user_rls": ["user_email"],
    "transactions": ["transaction_nsid"],
    "transactionline": [
        "transaction_nsid",
        "transaction_line_nsid",
    ],
}

# %%
# ==============================================================================
# DATA-QUALITY SQL DEFINITIONS
# ==============================================================================
# Relationship checks return the number of records that fail each parent-child
# or FX-coverage requirement.

RELATIONSHIP_CHECKS = {
    "Transaction lines missing transactions": """
        SELECT COUNT(*)
        FROM [stg].[transactionline] AS tl
        LEFT JOIN [stg].[transactions] AS t
            ON tl.transaction_nsid = t.transaction_nsid
        WHERE tl.transaction_nsid IS NOT NULL
          AND t.transaction_nsid IS NULL;
    """,

    "Transaction lines missing items": """
        SELECT COUNT(*)
        FROM [stg].[transactionline] AS tl
        LEFT JOIN [stg].[item] AS i
            ON tl.item_nsid = i.item_nsid
        WHERE tl.item_nsid IS NOT NULL
          AND i.item_nsid IS NULL;
    """,

    "Transactions missing customers": """
        SELECT COUNT(*)
        FROM [stg].[transactions] AS t
        LEFT JOIN [stg].[customer] AS c
            ON t.customer_nsid = c.customer_nsid
        WHERE t.customer_nsid IS NOT NULL
          AND c.customer_nsid IS NULL;
    """,

    "Items missing item categories": """
        SELECT COUNT(*)
        FROM [stg].[item] AS i
        LEFT JOIN [stg].[item_category] AS c
            ON i.item_category_nsid = c.item_category_nsid
        WHERE i.item_category_nsid IS NOT NULL
          AND c.item_category_nsid IS NULL;
    """,

    "Items missing item patterns": """
        SELECT COUNT(*)
        FROM [stg].[item] AS i
        LEFT JOIN [stg].[item_pattern] AS p
            ON i.item_pattern_nsid = p.item_pattern_nsid
        WHERE i.item_pattern_nsid IS NOT NULL
          AND p.item_pattern_nsid IS NULL;
    """,

    "Transactions missing subsidiaries": """
        SELECT COUNT(*)
        FROM [stg].[transactions] AS t
        LEFT JOIN [stg].[subsidiary] AS s
            ON t.bu_nsid = s.bu_nsid
        WHERE t.bu_nsid IS NOT NULL
          AND s.bu_nsid IS NULL;
    """,

    "Sales budgets missing subsidiaries": """
        SELECT COUNT(*)
        FROM [stg].[sales_budget] AS b
        LEFT JOIN [stg].[subsidiary] AS s
            ON b.bu_code = s.bu_code
        WHERE b.bu_code IS NOT NULL
          AND s.bu_code IS NULL;
    """,

    "Transaction currencies missing USD FX rates": """
        SELECT COUNT(*)
        FROM
        (
            SELECT DISTINCT foreign_currency
            FROM [stg].[transactionline]
            WHERE foreign_currency IS NOT NULL
        ) AS currencies
        LEFT JOIN
        (
            SELECT DISTINCT original_currency
            FROM [stg].[fx_avg_rate]
            WHERE target_currency = 'USD'
        ) AS fx
            ON currencies.foreign_currency = fx.original_currency
        WHERE fx.original_currency IS NULL;
    """,

    "Budget currencies missing USD FX rates": """
        SELECT COUNT(*)
        FROM
        (
            SELECT DISTINCT bu_currency
            FROM [stg].[sales_budget]
            WHERE bu_currency IS NOT NULL
        ) AS currencies
        LEFT JOIN
        (
            SELECT DISTINCT original_currency
            FROM [stg].[fx_avg_rate]
            WHERE target_currency = 'USD'
        ) AS fx
            ON currencies.bu_currency = fx.original_currency
        WHERE fx.original_currency IS NULL;
    """,
}


FX_DETAIL_CHECKS = {
    "Transaction currencies missing USD FX rates": """
        SELECT DISTINCT
            tl.foreign_currency
        FROM [stg].[transactionline] AS tl
        LEFT JOIN [stg].[fx_avg_rate] AS fx
            ON tl.foreign_currency = fx.original_currency
           AND fx.target_currency = 'USD'
        WHERE tl.foreign_currency IS NOT NULL
          AND fx.original_currency IS NULL
        ORDER BY tl.foreign_currency;
    """,

    "Budget currencies missing USD FX rates": """
        SELECT DISTINCT
            b.bu_currency
        FROM [stg].[sales_budget] AS b
        LEFT JOIN [stg].[fx_avg_rate] AS fx
            ON b.bu_currency = fx.original_currency
           AND fx.target_currency = 'USD'
        WHERE b.bu_currency IS NOT NULL
          AND fx.original_currency IS NULL
        ORDER BY b.bu_currency;
    """,
}

# Business-rule checks return the number of rows that violate each rule.

BUSINESS_RULE_CHECKS = {
    # --------------------------------------------------------------
    # TRANSACTIONS
    # Primary key: transaction_nsid
    # --------------------------------------------------------------

    "Duplicate transaction primary keys": """
        SELECT COALESCE(SUM(duplicate_count - 1), 0)
        FROM
        (
            SELECT
                transaction_nsid,
                COUNT(*) AS duplicate_count
            FROM [stg].[transactions]
            WHERE transaction_nsid IS NOT NULL
            GROUP BY transaction_nsid
            HAVING COUNT(*) > 1
        ) AS duplicates;
    """,

    "Invalid transaction type and status combinations": """
        SELECT COUNT(*)
        FROM [stg].[transactions]
        WHERE NOT
        (
            (
                transaction_type = 'Invoice'
                AND transaction_status = 'Fully Billed'
            )
            OR
            (
                transaction_type = 'Sales Order'
                AND transaction_status IN
                (
                    'Under Discussion',
                    'Ongoing',
                    'Closed - Won',
                    'Closed - Lost'
                )
            )
            OR
            (
                transaction_type = 'Opportunity'
                AND transaction_status IN
                (
                    'Under Discussion',
                    'Ongoing',
                    'Closed - Won',
                    'Closed - Lost'
                )
            )
        );
    """,

    "Transactions missing required fields": """
        SELECT COUNT(*)
        FROM [stg].[transactions]
        WHERE transaction_nsid IS NULL
           OR NULLIF(LTRIM(RTRIM(transaction_number)), '') IS NULL
           OR NULLIF(LTRIM(RTRIM(transaction_type)), '') IS NULL
           OR NULLIF(LTRIM(RTRIM(transaction_status)), '') IS NULL
           OR transaction_date IS NULL
           OR transaction_last_modified_date IS NULL
           OR bu_nsid IS NULL
           OR customer_nsid IS NULL;
    """,

    "Duplicate transaction numbers": """
        SELECT COALESCE(SUM(duplicate_count - 1), 0)
        FROM
        (
            SELECT
                transaction_number,
                COUNT(*) AS duplicate_count
            FROM [stg].[transactions]
            WHERE NULLIF(
                LTRIM(RTRIM(transaction_number)),
                ''
            ) IS NOT NULL
            GROUP BY transaction_number
            HAVING COUNT(*) > 1
        ) AS duplicates;
    """,

    "Invalid expected delivery date rules": """
        SELECT COUNT(*)
        FROM [stg].[transactions]
        WHERE
        (
            transaction_type = 'Invoice'
            AND expected_delivery_date IS NOT NULL
        )
        OR
        (
            transaction_type IN
            (
                'Sales Order',
                'Opportunity'
            )
            AND expected_delivery_date IS NULL
        );
    """,

    "Transaction modified before transaction date": """
        SELECT COUNT(*)
        FROM [stg].[transactions]
        WHERE transaction_last_modified_date < transaction_date;
    """,

    "Expected delivery before transaction date": """
        SELECT COUNT(*)
        FROM [stg].[transactions]
        WHERE expected_delivery_date IS NOT NULL
          AND expected_delivery_date < transaction_date;
    """,

    # --------------------------------------------------------------
    # TRANSACTION LINES
    # Primary key: transaction_nsid + transaction_line_nsid
    # --------------------------------------------------------------

    "Duplicate transaction-line primary keys": """
        SELECT COALESCE(SUM(duplicate_count - 1), 0)
        FROM
        (
            SELECT
                transaction_nsid,
                transaction_line_nsid,
                COUNT(*) AS duplicate_count
            FROM [stg].[transactionline]
            WHERE transaction_nsid IS NOT NULL
              AND transaction_line_nsid IS NOT NULL
            GROUP BY
                transaction_nsid,
                transaction_line_nsid
            HAVING COUNT(*) > 1
        ) AS duplicates;
    """,

    "Transaction lines missing required fields": """
        SELECT COUNT(*)
        FROM [stg].[transactionline]
        WHERE transaction_nsid IS NULL
           OR transaction_line_nsid IS NULL
           OR quantity IS NULL
           OR foreign_amount IS NULL
           OR NULLIF(LTRIM(RTRIM(foreign_currency)), '') IS NULL
           OR bu_rate IS NULL
           OR item_nsid IS NULL
           OR transaction_line_last_modified_date IS NULL;
    """,

    "Invalid transaction-line currency codes": """
        SELECT COUNT(*)
        FROM [stg].[transactionline]
        WHERE foreign_currency IS NOT NULL
          AND LEN(LTRIM(RTRIM(foreign_currency))) <> 3;
    """,

    "Invalid business-unit rates": """
        SELECT COUNT(*)
        FROM [stg].[transactionline]
        WHERE bu_rate <= 0;
    """,

    # --------------------------------------------------------------
    # CUSTOMERS
    # Primary key: customer_nsid
    # --------------------------------------------------------------

    "Duplicate customer primary keys": """
        SELECT COALESCE(SUM(duplicate_count - 1), 0)
        FROM
        (
            SELECT
                customer_nsid,
                COUNT(*) AS duplicate_count
            FROM [stg].[customer]
            WHERE customer_nsid IS NOT NULL
            GROUP BY customer_nsid
            HAVING COUNT(*) > 1
        ) AS duplicates;
    """,

    "Customers missing required fields": """
        SELECT COUNT(*)
        FROM [stg].[customer]
        WHERE customer_nsid IS NULL
           OR NULLIF(LTRIM(RTRIM(customer_name)), '') IS NULL
           OR NULLIF(LTRIM(RTRIM(customer_tier)), '') IS NULL;
    """,

    "Invalid customer tiers": """
        SELECT COUNT(*)
        FROM [stg].[customer]
        WHERE customer_tier NOT IN
        (
            'Bronze',
            'Silver',
            'Gold'
        );
    """,

    # --------------------------------------------------------------
    # ITEMS
    # Primary key: item_nsid
    # --------------------------------------------------------------

    "Duplicate item primary keys": """
        SELECT COALESCE(SUM(duplicate_count - 1), 0)
        FROM
        (
            SELECT
                item_nsid,
                COUNT(*) AS duplicate_count
            FROM [stg].[item]
            WHERE item_nsid IS NOT NULL
            GROUP BY item_nsid
            HAVING COUNT(*) > 1
        ) AS duplicates;
    """,

    "Items missing required fields": """
        SELECT COUNT(*)
        FROM [stg].[item]
        WHERE item_nsid IS NULL
           OR NULLIF(LTRIM(RTRIM(item_name)), '') IS NULL
           OR NULLIF(LTRIM(RTRIM(item_code)), '') IS NULL
           OR NULLIF(LTRIM(RTRIM(item_type)), '') IS NULL
           OR item_category_nsid IS NULL
           OR item_pattern_nsid IS NULL;
    """,

    "Invalid item types": """
        SELECT COUNT(*)
        FROM [stg].[item]
        WHERE item_type NOT IN
        (
            'Assembly',
            'Build'
        );
    """,

    # --------------------------------------------------------------
    # ITEM CATEGORIES
    # Primary key: item_category_nsid
    # --------------------------------------------------------------

    "Duplicate item-category primary keys": """
        SELECT COALESCE(SUM(duplicate_count - 1), 0)
        FROM
        (
            SELECT
                item_category_nsid,
                COUNT(*) AS duplicate_count
            FROM [stg].[item_category]
            WHERE item_category_nsid IS NOT NULL
            GROUP BY item_category_nsid
            HAVING COUNT(*) > 1
        ) AS duplicates;
    """,

    "Item categories missing required fields": """
        SELECT COUNT(*)
        FROM [stg].[item_category]
        WHERE item_category_nsid IS NULL
           OR NULLIF(LTRIM(RTRIM(item_category)), '') IS NULL
           OR NULLIF(LTRIM(RTRIM(item_sub_category)), '') IS NULL;
    """,

    # --------------------------------------------------------------
    # ITEM PATTERNS
    # Primary key: item_pattern_nsid
    # --------------------------------------------------------------

    "Duplicate item-pattern primary keys": """
        SELECT COALESCE(SUM(duplicate_count - 1), 0)
        FROM
        (
            SELECT
                item_pattern_nsid,
                COUNT(*) AS duplicate_count
            FROM [stg].[item_pattern]
            WHERE item_pattern_nsid IS NOT NULL
            GROUP BY item_pattern_nsid
            HAVING COUNT(*) > 1
        ) AS duplicates;
    """,

    "Item patterns missing required fields": """
        SELECT COUNT(*)
        FROM [stg].[item_pattern]
        WHERE item_pattern_nsid IS NULL
           OR NULLIF(LTRIM(RTRIM(item_pattern)), '') IS NULL;
    """,

    # --------------------------------------------------------------
    # SUBSIDIARIES
    # Primary key: bu_nsid
    # --------------------------------------------------------------

    "Duplicate subsidiary primary keys": """
        SELECT COALESCE(SUM(duplicate_count - 1), 0)
        FROM
        (
            SELECT
                bu_nsid,
                COUNT(*) AS duplicate_count
            FROM [stg].[subsidiary]
            WHERE bu_nsid IS NOT NULL
            GROUP BY bu_nsid
            HAVING COUNT(*) > 1
        ) AS duplicates;
    """,

    "Subsidiaries missing required fields": """
        SELECT COUNT(*)
        FROM [stg].[subsidiary]
        WHERE bu_nsid IS NULL
           OR NULLIF(LTRIM(RTRIM(bu_code)), '') IS NULL
           OR NULLIF(LTRIM(RTRIM(bu_currency)), '') IS NULL;
    """,

    # --------------------------------------------------------------
    # SALES BUDGET
    # Composite primary key:
    # budget_version + budget_date + customer_name + bu_code
    # --------------------------------------------------------------

    "Duplicate sales-budget primary keys": """
        SELECT COALESCE(SUM(duplicate_count - 1), 0)
        FROM
        (
            SELECT
                UPPER(LTRIM(RTRIM(budget_version)))
                    AS budget_version,
                budget_date,
                UPPER(LTRIM(RTRIM(customer_name)))
                    AS customer_name,
                UPPER(LTRIM(RTRIM(bu_code)))
                    AS bu_code,
                COUNT(*) AS duplicate_count
            FROM [stg].[sales_budget]
            WHERE NULLIF(
                    LTRIM(RTRIM(budget_version)),
                    ''
                  ) IS NOT NULL
              AND budget_date IS NOT NULL
              AND NULLIF(
                    LTRIM(RTRIM(customer_name)),
                    ''
                  ) IS NOT NULL
              AND NULLIF(
                    LTRIM(RTRIM(bu_code)),
                    ''
                  ) IS NOT NULL
            GROUP BY
                UPPER(LTRIM(RTRIM(budget_version))),
                budget_date,
                UPPER(LTRIM(RTRIM(customer_name))),
                UPPER(LTRIM(RTRIM(bu_code)))
            HAVING COUNT(*) > 1
        ) AS duplicates;
    """,

    "Sales budgets missing required fields": """
        SELECT COUNT(*)
        FROM [stg].[sales_budget]
        WHERE budget_year IS NULL
           OR NULLIF(LTRIM(RTRIM(budget_version)), '') IS NULL
           OR budget_date IS NULL
           OR NULLIF(LTRIM(RTRIM(customer_name)), '') IS NULL
           OR NULLIF(LTRIM(RTRIM(bu_code)), '') IS NULL
           OR NULLIF(LTRIM(RTRIM(bu_currency)), '') IS NULL
           OR sales_amount_bu_currency IS NULL;
    """,

    "Budget year does not match budget date": """
        SELECT COUNT(*)
        FROM [stg].[sales_budget]
        WHERE budget_year <> YEAR(budget_date);
    """,

    "Budget dates are not month end": """
        SELECT COUNT(*)
        FROM [stg].[sales_budget]
        WHERE budget_date <> EOMONTH(budget_date);
    """,

    "Negative sales budget amounts": """
        SELECT COUNT(*)
        FROM [stg].[sales_budget]
        WHERE sales_amount_bu_currency < 0;
    """,

    "Invalid budget currency codes": """
        SELECT COUNT(*)
        FROM [stg].[sales_budget]
        WHERE bu_currency IS NOT NULL
          AND LEN(LTRIM(RTRIM(bu_currency))) <> 3;
    """,

    # --------------------------------------------------------------
    # FX RATES
    # Composite primary key:
    # original_currency + target_currency + rate_date
    # --------------------------------------------------------------

    "Duplicate FX-rate primary keys": """
        SELECT COALESCE(SUM(duplicate_count - 1), 0)
        FROM
        (
            SELECT
                UPPER(LTRIM(RTRIM(original_currency)))
                    AS original_currency,
                UPPER(LTRIM(RTRIM(target_currency)))
                    AS target_currency,
                rate_date,
                COUNT(*) AS duplicate_count
            FROM [stg].[fx_avg_rate]
            WHERE NULLIF(
                    LTRIM(RTRIM(original_currency)),
                    ''
                  ) IS NOT NULL
              AND NULLIF(
                    LTRIM(RTRIM(target_currency)),
                    ''
                  ) IS NOT NULL
              AND rate_date IS NOT NULL
            GROUP BY
                UPPER(LTRIM(RTRIM(original_currency))),
                UPPER(LTRIM(RTRIM(target_currency))),
                rate_date
            HAVING COUNT(*) > 1
        ) AS duplicates;
    """,

    "FX rates missing required fields": """
        SELECT COUNT(*)
        FROM [stg].[fx_avg_rate]
        WHERE NULLIF(LTRIM(RTRIM(original_currency)), '') IS NULL
           OR NULLIF(LTRIM(RTRIM(target_currency)), '') IS NULL
           OR rate_date IS NULL
           OR avg_rate IS NULL;
    """,

    "Invalid FX currency codes": """
        SELECT COUNT(*)
        FROM [stg].[fx_avg_rate]
        WHERE LEN(LTRIM(RTRIM(original_currency))) <> 3
           OR LEN(LTRIM(RTRIM(target_currency))) <> 3;
    """,

    "Invalid FX rates": """
        SELECT COUNT(*)
        FROM [stg].[fx_avg_rate]
        WHERE avg_rate <= 0;
    """,

    "FX dates are not month end": """
        SELECT COUNT(*)
        FROM [stg].[fx_avg_rate]
        WHERE rate_date <> EOMONTH(rate_date);
    """,

    "Unsupported FX target currencies": """
        SELECT COUNT(*)
        FROM [stg].[fx_avg_rate]
        WHERE target_currency NOT IN
        (
            'USD',
            'EUR'
        );
    """,

    # --------------------------------------------------------------
    # DELETED RECORDS
    # Logical key: transaction_nsid
    # --------------------------------------------------------------

    "Duplicate deleted transaction IDs": """
        SELECT COALESCE(SUM(duplicate_count - 1), 0)
        FROM
        (
            SELECT
                transaction_nsid,
                COUNT(*) AS duplicate_count
            FROM [stg].[deleted_records]
            WHERE transaction_nsid IS NOT NULL
            GROUP BY transaction_nsid
            HAVING COUNT(*) > 1
        ) AS duplicates;
    """,

    "Deleted records missing required fields": """
        SELECT COUNT(*)
        FROM [stg].[deleted_records]
        WHERE transaction_nsid IS NULL
           OR deleted_date IS NULL;
    """,

    "Deleted records have future deletion dates": """
        SELECT COUNT(*)
        FROM [stg].[deleted_records]
        WHERE deleted_date > CAST(GETDATE() AS DATE);
    """,
}

# %%
# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def run_relationship_checks(
    engine: Engine,
) -> tuple[dict[str, int], dict[str, list[str]]]:
    """Run relationship checks and retrieve FX failure details."""

    results: dict[str, int] = {}
    fx_details: dict[str, list[str]] = {}

    with engine.connect() as connection:
        for check_name, query in RELATIONSHIP_CHECKS.items():
            count = connection.execute(text(query)).scalar_one()
            results[check_name] = count

            if count > 0 and check_name in FX_DETAIL_CHECKS:
                missing_currencies = connection.execute(
                    text(FX_DETAIL_CHECKS[check_name])
                ).scalars().all()

                fx_details[check_name] = list(missing_currencies)

    return results, fx_details


def print_relationship_check_results(
    results: dict[str, int],
    fx_details: dict[str, list[str]],
) -> None:
    """Print relationship-check results."""

    print("Relationship check results")
    print("-" * 60)

    for check_name, count in results.items():
        status = "PASS" if count == 0 else "FAIL"
        print(f"[{status}] {check_name}: {count:,}")

        if check_name in fx_details:
            missing = ", ".join(fx_details[check_name])
            print(f"       Missing currencies: {missing}")


def run_business_rule_checks(
    engine: Engine,
) -> dict[str, int]:
    """Run business-rule validation checks."""

    results: dict[str, int] = {}

    with engine.connect() as connection:
        for check_name, query in BUSINESS_RULE_CHECKS.items():
            count = connection.execute(text(query)).scalar_one()
            results[check_name] = count

    return results


def print_business_rule_results(
    results: dict[str, int],
) -> None:
    """Print business-rule validation results."""

    print("\nBusiness rule check results")
    print("-" * 60)

    for check_name, count in results.items():
        status = "PASS" if count == 0 else "FAIL"
        print(f"[{status}] {check_name}: {count:,}")


def finish_pipeline_run(
    engine: Engine,
    pipeline_run_id: str,
    status: str,
) -> None:
    """Close the pipeline audit record."""

    if status not in {"SUCCESS", "FAILED"}:
        raise ValueError("Status must be SUCCESS or FAILED.")

    with engine.begin() as connection:
        connection.execute(
            text("""
                UPDATE [audit].[pipeline_run]
                SET
                    end_time = SYSUTCDATETIME(),
                    status = :status
                WHERE pipeline_run_id = :pipeline_run_id;
            """),
            {
                "pipeline_run_id": pipeline_run_id,
                "status": status,
            },
        )


def save_data_quality_results(
    engine: Engine,
    pipeline_run_id: str,
    results: dict[str, int],
    check_category: str,
    severity: str,
) -> None:
    """Insert validation results into the audit table."""

    insert_query = text("""
        INSERT INTO [audit].[data_quality_result]
        (
            pipeline_run_id,
            check_name,
            check_category,
            severity,
            failed_row_count,
            status,
            check_timestamp
        )
        VALUES
        (
            :pipeline_run_id,
            :check_name,
            :check_category,
            :severity,
            :failed_row_count,
            :status,
            :check_timestamp
        );
    """)

    with engine.begin() as connection:
        for check_name, failed_row_count in results.items():
            connection.execute(
                insert_query,
                {
                    "pipeline_run_id": pipeline_run_id,
                    "check_name": check_name,
                    "check_category": check_category,
                    "severity": severity,
                    "failed_row_count": failed_row_count,
                    "status": (
                        "PASS"
                        if failed_row_count == 0
                        else "FAIL"
                    ),
                    "check_timestamp": datetime.now(),
                },
            )


def enforce_data_quality_gate(
    relationship_results: dict[str, int],
    business_rule_results: dict[str, int],
) -> None:
    """Stop the pipeline if any critical check failed."""

    failed_checks: dict[str, int] = {}

    for check_name, failed_row_count in relationship_results.items():
        if failed_row_count > 0:
            failed_checks[check_name] = failed_row_count

    for check_name, failed_row_count in business_rule_results.items():
        if failed_row_count > 0:
            failed_checks[check_name] = failed_row_count

    if failed_checks:
        failure_summary = "; ".join(
            f"{check_name}: {failed_row_count}"
            for check_name, failed_row_count in failed_checks.items()
        )

        raise RuntimeError(
            f"Data quality gate failed. {failure_summary}"
        )

    print("Data quality gate passed.")


def run_dwh_loads(
    engine: Engine,
    pipeline_run_id: str,
) -> None:
    """Execute the current-state DWH load procedures in dependency order."""

    procedures = [
        "dwh.usp_load_customer",
        "dwh.usp_load_item_category",
        "dwh.usp_load_item_pattern",
        "dwh.usp_load_item",
        "dwh.usp_load_subsidiary",
        "dwh.usp_load_sales_budget",
        "dwh.usp_load_fx_avg_rate",
        "dwh.usp_load_transactions",
    ]

    for procedure_name in procedures:
        with engine.begin() as connection:
            connection.execute(
                text(
                    f"""
                    EXEC {procedure_name}
                        @pipeline_run_id = :pipeline_run_id;
                    """
                ),
                {
                    "pipeline_run_id": pipeline_run_id,
                },
            )

        print(f"Completed: {procedure_name}")


def run_scd_loads(
    engine: Engine,
    pipeline_run_id: str,
) -> None:
    """Execute the SCD Type 2 procedures for customer and item history."""

    procedures = [
        "scd.usp_load_customer",
        "scd.usp_load_item",
    ]

    for procedure_name in procedures:
        with engine.begin() as connection:
            connection.execute(
                text(
                    f"""
                    EXEC {procedure_name}
                        @pipeline_run_id = :pipeline_run_id;
                    """
                ),
                {
                    "pipeline_run_id": pipeline_run_id,
                },
            )

        print(f"Completed: {procedure_name}")

# %%
# ==============================================================================
# PIPELINE EXECUTION: RAW INGESTION
# ==============================================================================

directory_path = DIRECTORY_PATH
raw_tables = RAW_TABLES

pipeline_run_id = str(uuid4())
pipeline_start_time = datetime.now()
pipeline_name = "netsuite_raw_ingestion"


# Insert the pipeline audit record used by the remaining processing steps.
with engine.begin() as connection:
    connection.execute(
        text("""
            INSERT INTO audit.pipeline_run
            (
                pipeline_run_id,
                pipeline_name,
                start_time,
                status
            )
            VALUES
            (
                :pipeline_run_id,
                :pipeline_name,
                :start_time,
                'RUNNING'
            )
        """),
        {
            "pipeline_run_id": pipeline_run_id,
            "pipeline_name": pipeline_name,
            "start_time": pipeline_start_time,
        },
    )


dataframes = {}

# ------------------------------------------------------------------
# LOAD RAW FILES
# ------------------------------------------------------------------

try:

    for file_path in directory_path.glob("*.csv"):

        file_name = file_path.stem.lower()

        # Ignore files that are not part of the pipeline.
        if file_name not in raw_tables:
            print(f"Skipped: {file_path.name}")
            continue

        table_start_time = datetime.now()
        table_name = raw_tables[file_name]

        # Read the original CSV.
        df = pl.read_csv(
            file_path,
            try_parse_dates=True,
            separator=";",
        )
        source_row_count = df.height


        # ----------------------------------------------------------
        # FX RATE TRANSFORMATION
        # ----------------------------------------------------------

        if file_name == "fx_avg_rate":

            # Dynamically identify every column whose name is a date.
            # This works whether the file contains 1, 3 or many months.
            date_columns = [
                column
                for column in df.columns
                if re.fullmatch(r"\d{2}/\d{2}/\d{4}", column.strip())
            ]

            if not date_columns:
                raise ValueError(
                    "No FX date columns were found in fx_avg_rate.csv"
                )

            df = (
                df.unpivot(
                    index=["original_currency", "target_currency"],
                    on=date_columns,
                    variable_name="rate_date",
                    value_name="avg_rate",
                )
                .with_columns(
                    pl.col("rate_date").str.strptime(
                        pl.Date,
                        format="%d/%m/%Y",
                        strict=True,
                    ),
                    pl.col("avg_rate").cast(
                        pl.Float64,
                        strict=False,
                    ),
                )
            )

        # ----------------------------------------------------------
        # ADD INGESTION METADATA TO DATAFRAMES
        # ----------------------------------------------------------

        ingestion_timestamp = datetime.now()

        df = df.with_columns(
            pl.lit(pipeline_run_id).alias("pipeline_run_id"),
            pl.lit(ingestion_timestamp).alias(
                "ingestion_timestamp"
            ),
            pl.lit(file_path.name).alias("source_file_name"),
        )

        dataframes[file_name] = df


        # ----------------------------------------------------------
        # LOAD TABLE AND WRITE AUDIT RECORD
        # ----------------------------------------------------------

        # Everything inside this block is committed together.
        # If the insert or audit fails, the table transaction rolls back.
        with engine.begin() as connection:

            # The CSV files represent the latest raw snapshot.
            # Keep the table structure but remove the previous data.
            connection.execute(text(f"DELETE FROM {table_name}"))

            # Insert the new raw data.
            df.write_database(
                table_name=table_name,
                connection=connection,
                if_table_exists="append",
            )

            # Count the records loaded by this pipeline run.
            loaded_row_count = connection.execute(
                text(f"""
                    SELECT COUNT(*)
                    FROM {table_name}
                    WHERE pipeline_run_id = :pipeline_run_id
                """),
                {
                    "pipeline_run_id": pipeline_run_id,
                },
            ).scalar_one()

            # Ensure the final DataFrame count matches SQL Server.
            if loaded_row_count != df.height:
                raise ValueError(
                    f"Row-count mismatch for {table_name}. "
                    f"Expected {df.height}, loaded {loaded_row_count}."
                )

            # Write one successful audit record for this table.
            connection.execute(
                text("""
                    INSERT INTO audit.table_load
                    (
                        pipeline_run_id,
                        table_name,
                        source_file_name,
                        source_row_count,
                        loaded_row_count,
                        start_time,
                        end_time,
                        status
                    )
                    VALUES
                    (
                        :pipeline_run_id,
                        :table_name,
                        :source_file_name,
                        :source_row_count,
                        :loaded_row_count,
                        :start_time,
                        :end_time,
                        'SUCCESS'
                    )
                """),
                {
                    "pipeline_run_id": pipeline_run_id,
                    "table_name": table_name,
                    "source_file_name": file_path.name,
                    "source_row_count": source_row_count,
                    "loaded_row_count": loaded_row_count,
                    "start_time": table_start_time,
                    "end_time": datetime.now(),
                },
            )

        print(
            f"Loaded {file_path.name} into {table_name}: "
            f"{loaded_row_count} rows"
        )


    # --------------------------------------------------------------
    # MARK PIPELINE AS SUCCESSFUL
    # --------------------------------------------------------------

    with engine.begin() as connection:
        connection.execute(
            text("""
                UPDATE audit.pipeline_run
                SET
                    end_time = :end_time,
                    status = 'SUCCESS'
                WHERE pipeline_run_id = :pipeline_run_id
            """),
            {
                "pipeline_run_id": pipeline_run_id,
                "end_time": datetime.now(),
            },
        )

    print(f"Pipeline completed successfully: {pipeline_run_id}")

# ------------------------------------------------------------------
# PIPELINE FAILURE HANDLING
# ------------------------------------------------------------------

except Exception as error:

    with engine.begin() as connection:
        connection.execute(
            text("""
                UPDATE audit.pipeline_run
                SET
                    end_time = :end_time,
                    status = 'FAILED',
                    error_message = :error_message
                WHERE pipeline_run_id = :pipeline_run_id
            """),
            {
                "pipeline_run_id": pipeline_run_id,
                "end_time": datetime.now(),
                "error_message": str(error),
            },
        )

    print(f"Pipeline failed: {error}")

    raise

# %%
# ==============================================================================
# PIPELINE EXECUTION: STAGING PRIMARY-KEY VALIDATION
# ==============================================================================
# Inspect every configured staging view for NULL keys and duplicate key groups.

primary_keys = PRIMARY_KEYS

# Store validation results for each staging view.
results = []

with engine.connect() as connection:

    # Retrieve all views in the staging schema.
    stg_views = connection.execute(
        text("""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = 'stg'
            ORDER BY TABLE_NAME
        """)
    ).scalars().all()

    for table_name in stg_views:

        # Skip views that do not have a configured key.
        keys = primary_keys.get(table_name)

        if not keys:
            print(f"No key configured for stg.{table_name}")
            continue

        # Quote identifiers to handle reserved words such as transaction.
        qualified_table = f"[stg].[{table_name}]"

        key_columns = ", ".join(
            f"[{column}]"
            for column in keys
        )

        # A composite key is invalid when any key column is NULL.
        null_condition = " OR ".join(
            f"[{column}] IS NULL"
            for column in keys
        )

        # Exclude NULL keys from the duplicate check.
        non_null_condition = " AND ".join(
            f"[{column}] IS NOT NULL"
            for column in keys
        )

        # Count all rows in the staging view.
        row_count = connection.execute(
            text(f"""
                SELECT COUNT(*)
                FROM {qualified_table}
            """)
        ).scalar_one()

        # Count rows containing NULL primary key values.
        null_key_count = connection.execute(
            text(f"""
                SELECT COUNT(*)
                FROM {qualified_table}
                WHERE {null_condition}
            """)
        ).scalar_one()

        # Find primary key values that occur more than once.
        duplicates = connection.execute(
            text(f"""
                SELECT
                    {key_columns},
                    COUNT(*) AS duplicate_count
                FROM {qualified_table}
                WHERE {non_null_condition}
                GROUP BY {key_columns}
                HAVING COUNT(*) > 1
            """)
        ).mappings().all()

        results.append(
            {
                "table_name": f"stg.{table_name}",
                "primary_key": keys,
                "row_count": row_count,
                "null_key_count": null_key_count,
                "duplicate_groups": len(duplicates),
                "duplicates": duplicates,
            }
        )


# Print a summary and the details of any duplicate keys.
for result in results:
    print(f"\nTable: {result['table_name']}")
    print(f"Key: {', '.join(result['primary_key'])}")
    print(f"Rows: {result['row_count']}")
    print(f"NULL keys: {result['null_key_count']}")
    print(f"Duplicate groups: {result['duplicate_groups']}")

    for duplicate in result["duplicates"]:
        print(dict(duplicate))

# %%
# ==============================================================================
# PIPELINE EXECUTION: RELATIONSHIP CHECKS
# ==============================================================================
# Validate referential integrity and report the currencies that lack USD FX
# coverage.

relationship_results, fx_missing_details = run_relationship_checks(engine)

print_relationship_check_results(
    relationship_results,
    fx_missing_details,
)

# %%
# ==============================================================================
# PIPELINE EXECUTION: BUSINESS-RULE CHECKS
# ==============================================================================
# Validate allowed values, required fields, dates, currencies, rates, and keys.

business_rule_results = run_business_rule_checks(
    engine
)

print_business_rule_results(
    business_rule_results
)

# %%
# ==============================================================================
# PIPELINE EXECUTION: QUALITY GATE AND WAREHOUSE LOADS
# ==============================================================================
# Stop on critical DQ failures, then preserve SCD history and load the current
# source-aligned warehouse tables.

try:
    enforce_data_quality_gate(
        relationship_results=relationship_results,
        business_rule_results=business_rule_results,
    )

    # Preserve customer and item history.
    run_scd_loads(
        engine=engine,
        pipeline_run_id=pipeline_run_id,
    )

    # Load current source-aligned warehouse tables.
    run_dwh_loads(
        engine=engine,
        pipeline_run_id=pipeline_run_id,
    )

    # Mark the complete pipeline as successful.
    with engine.begin() as connection:
        connection.execute(
            text("""
                UPDATE audit.pipeline_run
                SET
                    end_time = :end_time,
                    status = 'SUCCESS',
                    error_message = NULL
                WHERE pipeline_run_id = :pipeline_run_id
            """),
            {
                "pipeline_run_id": pipeline_run_id,
                "end_time": datetime.now(),
            },
        )

    print(f"Pipeline completed successfully: {pipeline_run_id}")

except Exception as error:
    with engine.begin() as connection:
        connection.execute(
            text("""
                UPDATE audit.pipeline_run
                SET
                    end_time = :end_time,
                    status = 'FAILED',
                    error_message = :error_message
                WHERE pipeline_run_id = :pipeline_run_id
            """),
            {
                "pipeline_run_id": pipeline_run_id,
                "end_time": datetime.now(),
                "error_message": str(error),
            },
        )

    print(f"Pipeline failed: {error}")

    raise