/*==============================================================
  Customer
==============================================================*/
CREATE OR ALTER VIEW stg.customer
AS
SELECT
    customer_nsid,
    NULLIF(LTRIM(RTRIM(customer_name)), '') AS customer_name,
    NULLIF(LTRIM(RTRIM(customer_tier)), '') AS customer_tier,
    pipeline_run_id,
    ingestion_timestamp,
    source_file_name
FROM raw.customer;
GO


/*==============================================================
  Deleted Records
==============================================================*/
CREATE OR ALTER VIEW stg.deleted_records
AS
SELECT
    transaction_nsid,
    deleted_date,
    pipeline_run_id,
    ingestion_timestamp,
    source_file_name
FROM raw.deleted_records;
GO


/*==============================================================
  FX Average Rate
==============================================================*/
CREATE OR ALTER VIEW stg.fx_avg_rate
AS
SELECT
    UPPER(NULLIF(LTRIM(RTRIM(original_currency)), '')) AS original_currency,
    UPPER(NULLIF(LTRIM(RTRIM(target_currency)), '')) AS target_currency,
    rate_date,
    avg_rate,
    pipeline_run_id,
    ingestion_timestamp,
    source_file_name
FROM raw.fx_avg_rate;
GO


/*==============================================================
  Item
==============================================================*/
CREATE OR ALTER VIEW stg.item
AS
SELECT
    item_nsid,
    item_category_nsid,
    item_pattern_nsid,
    NULLIF(LTRIM(RTRIM(item_name)), '') AS item_name,
    NULLIF(LTRIM(RTRIM(item_code)), '') AS item_code,
    NULLIF(LTRIM(RTRIM(item_type)), '') AS item_type,
    NULLIF(LTRIM(RTRIM(project_code)), '') AS project_code,
    pipeline_run_id,
    ingestion_timestamp,
    source_file_name
FROM raw.item;
GO


/*==============================================================
  Item Category
==============================================================*/
CREATE OR ALTER VIEW stg.item_category
AS
SELECT
    item_category_nsid,
    NULLIF(LTRIM(RTRIM(item_category)), '') AS item_category,
    NULLIF(LTRIM(RTRIM(item_sub_category)), '') AS item_sub_category,
    pipeline_run_id,
    ingestion_timestamp,
    source_file_name
FROM raw.item_category;
GO


/*==============================================================
  Item Pattern
==============================================================*/
CREATE OR ALTER VIEW stg.item_pattern
AS
SELECT
    item_pattern_nsid,
    NULLIF(LTRIM(RTRIM(item_pattern)), '') AS item_pattern,
    pipeline_run_id,
    ingestion_timestamp,
    source_file_name
FROM raw.item_pattern;
GO


/*==============================================================
  Sales Budget
==============================================================*/
CREATE OR ALTER VIEW stg.sales_budget
AS
SELECT
    sales_budget_id,
    NULLIF(LTRIM(RTRIM(customer_name)), '') AS customer_name,
    UPPER(NULLIF(LTRIM(RTRIM(bu_code)), '')) AS bu_code,
    UPPER(NULLIF(LTRIM(RTRIM(bu_currency)), '')) AS bu_currency,
    budget_year,
    NULLIF(LTRIM(RTRIM(budget_version)), '') AS budget_version,
    budget_date,
    sales_amount_bu_currency,
    pipeline_run_id,
    ingestion_timestamp,
    source_file_name
FROM raw.sales_budget;
GO


/*==============================================================
  Subsidiary / Business Unit
==============================================================*/
CREATE OR ALTER VIEW stg.subsidiary
AS
SELECT
    bu_nsid,
    UPPER(NULLIF(LTRIM(RTRIM(bu_code)), '')) AS bu_code,
    UPPER(NULLIF(LTRIM(RTRIM(bu_currency)), '')) AS bu_currency,
    UPPER(NULLIF(LTRIM(RTRIM(bu_country_code)), '')) AS bu_country_code,
    NULLIF(LTRIM(RTRIM(bu_legal_name)), '') AS bu_legal_name,
    NULLIF(LTRIM(RTRIM(bu_commercial_group)), '') AS bu_commercial_group,
    pipeline_run_id,
    ingestion_timestamp,
    source_file_name
FROM raw.subsidiary;
GO


/*==============================================================
  Transaction
==============================================================*/
CREATE OR ALTER VIEW stg.transactions
AS
SELECT
    transaction_nsid,
    bu_nsid,
    customer_nsid,
    NULLIF(LTRIM(RTRIM(transaction_type)), '') AS transaction_type,
    NULLIF(LTRIM(RTRIM(transaction_status)), '') AS transaction_status,
    NULLIF(LTRIM(RTRIM(transaction_number)), '') AS transaction_number,
    transaction_date,
    transaction_last_modified_date,
    expected_delivery_date,
    pipeline_run_id,
    ingestion_timestamp,
    source_file_name
FROM raw.transactions;
GO


/*==============================================================
  Transaction Line
==============================================================*/
CREATE OR ALTER VIEW stg.transactionline
AS
SELECT
    transaction_nsid,
    item_nsid,
    transaction_line_nsid,
    quantity,
    foreign_amount,
    UPPER(NULLIF(LTRIM(RTRIM(foreign_currency)), '')) AS foreign_currency,
    bu_rate,
    transaction_line_last_modified_date,
    pipeline_run_id,
    ingestion_timestamp,
    source_file_name
FROM raw.transactionline;
GO

CREATE OR ALTER VIEW stg.user_rls
AS
WITH LatestPipelineRun AS
(
    SELECT TOP (1)
        pipeline_run_id
    FROM raw.user_rls
    WHERE pipeline_run_id IS NOT NULL
    ORDER BY ingestion_timestamp DESC
),
NormalizedRules AS
(
    SELECT
        LOWER(
            NULLIF(
                LTRIM(RTRIM(user_email)),
                ''
            )
        ) AS user_email,

        NULLIF(
            LTRIM(RTRIM(authorized_bu_code)),
            ''
        ) AS authorized_bu_code,

        NULLIF(
            LTRIM(RTRIM(authorized_customer_name)),
            ''
        ) AS authorized_customer_name,

        NULLIF(
            LTRIM(RTRIM(authorized_item_type)),
            ''
        ) AS authorized_item_type,

        pipeline_run_id,
        ingestion_timestamp,
        source_file_name,

        ROW_NUMBER() OVER
        (
            PARTITION BY
                LOWER(LTRIM(RTRIM(user_email))),
                UPPER(LTRIM(RTRIM(authorized_bu_code))),
                UPPER(LTRIM(RTRIM(authorized_customer_name))),
                UPPER(LTRIM(RTRIM(authorized_item_type)))
            ORDER BY ingestion_timestamp DESC
        ) AS row_number

    FROM raw.user_rls
    WHERE pipeline_run_id =
    (
        SELECT pipeline_run_id
        FROM LatestPipelineRun
    )
)
SELECT
    user_email,
    authorized_bu_code,
    authorized_customer_name,
    authorized_item_type,
    pipeline_run_id,
    ingestion_timestamp,
    source_file_name
FROM NormalizedRules
WHERE row_number = 1
  AND user_email IS NOT NULL
  AND authorized_bu_code IS NOT NULL
  AND authorized_customer_name IS NOT NULL
  AND authorized_item_type IS NOT NULL;
GO