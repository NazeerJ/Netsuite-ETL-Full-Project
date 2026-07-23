/*==============================================================
  Customer
==============================================================*/
CREATE OR ALTER VIEW dbo.stg_customer
AS
SELECT
    customer_nsid,
    NULLIF(LTRIM(RTRIM(customer_name)), '') AS customer_name,
    NULLIF(LTRIM(RTRIM(customer_tier)), '') AS customer_tier,
    pipeline_run_id,
    ingestion_timestamp,
    source_file_name
FROM dbo.raw_customer;
GO


/*==============================================================
  Deleted Records
==============================================================*/
CREATE OR ALTER VIEW dbo.stg_deleted_records
AS
SELECT
    transaction_nsid,
    deleted_date,
    pipeline_run_id,
    ingestion_timestamp,
    source_file_name
FROM dbo.raw_deleted_records;
GO


/*==============================================================
  FX Average Rate
==============================================================*/
CREATE OR ALTER VIEW dbo.stg_fx_avg_rate
AS
SELECT
    UPPER(NULLIF(LTRIM(RTRIM(original_currency)), '')) AS original_currency,
    UPPER(NULLIF(LTRIM(RTRIM(target_currency)), '')) AS target_currency,
    rate_date,
    avg_rate,
    pipeline_run_id,
    ingestion_timestamp,
    source_file_name
FROM dbo.raw_fx_avg_rate;
GO


/*==============================================================
  Item
==============================================================*/
CREATE OR ALTER VIEW dbo.stg_item
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
FROM dbo.raw_item;
GO


/*==============================================================
  Item Category
==============================================================*/
CREATE OR ALTER VIEW dbo.stg_item_category
AS
SELECT
    item_category_nsid,
    NULLIF(LTRIM(RTRIM(item_category)), '') AS item_category,
    NULLIF(LTRIM(RTRIM(item_sub_category)), '') AS item_sub_category,
    pipeline_run_id,
    ingestion_timestamp,
    source_file_name
FROM dbo.raw_item_category;
GO


/*==============================================================
  Item Pattern
==============================================================*/
CREATE OR ALTER VIEW dbo.stg_item_pattern
AS
SELECT
    item_pattern_nsid,
    NULLIF(LTRIM(RTRIM(item_pattern)), '') AS item_pattern,
    pipeline_run_id,
    ingestion_timestamp,
    source_file_name
FROM dbo.raw_item_pattern;
GO


/*==============================================================
  Sales Budget
==============================================================*/
CREATE OR ALTER VIEW dbo.stg_sales_budget
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
FROM dbo.raw_sales_budget;
GO


/*==============================================================
  Subsidiary / Business Unit
==============================================================*/
CREATE OR ALTER VIEW dbo.stg_subsidiary
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
FROM dbo.raw_subsidiary;
GO


/*==============================================================
  Transaction
==============================================================*/
CREATE OR ALTER VIEW dbo.stg_transaction
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
FROM dbo.raw_transaction;
GO


/*==============================================================
  Transaction Line
==============================================================*/
CREATE OR ALTER VIEW dbo.stg_transactionline
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
FROM dbo.raw_transactionline;
GO