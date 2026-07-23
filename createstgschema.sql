CREATE TABLE raw_customer (
    customer_nsid BIGINT,
    customer_name VARCHAR(255),
    customer_tier VARCHAR(100),
    pipeline_run_id UNIQUEIDENTIFIER,
    ingestion_timestamp DATETIME2 DEFAULT SYSUTCDATETIME(),
    source_file_name VARCHAR(255)
);

CREATE TABLE raw_item_category (
    item_category_nsid BIGINT,
    item_category VARCHAR(255),
    item_sub_category VARCHAR(255),
    pipeline_run_id UNIQUEIDENTIFIER,
    ingestion_timestamp DATETIME2 DEFAULT SYSUTCDATETIME(),
    source_file_name VARCHAR(255)
);

CREATE TABLE raw_item_pattern (
    item_pattern_nsid BIGINT,
    item_pattern VARCHAR(255),
    pipeline_run_id UNIQUEIDENTIFIER,
    ingestion_timestamp DATETIME2 DEFAULT SYSUTCDATETIME(),
    source_file_name VARCHAR(255)
);

CREATE TABLE raw_item (
    item_nsid BIGINT,
    item_category_nsid BIGINT,
    item_pattern_nsid BIGINT,
    item_name VARCHAR(255),
    item_code VARCHAR(100),
    item_type VARCHAR(100),
    project_code VARCHAR(100),
    pipeline_run_id UNIQUEIDENTIFIER,
    ingestion_timestamp DATETIME2 DEFAULT SYSUTCDATETIME(),
    source_file_name VARCHAR(255)
);

CREATE TABLE raw_subsidiary (
    bu_nsid BIGINT,
    bu_code VARCHAR(50),
    bu_currency VARCHAR(3),
    bu_country_code VARCHAR(10),
    bu_legal_name VARCHAR(255),
    bu_commercial_group VARCHAR(255),
    pipeline_run_id UNIQUEIDENTIFIER,
    ingestion_timestamp DATETIME2 DEFAULT SYSUTCDATETIME(),
    source_file_name VARCHAR(255)
);

CREATE TABLE raw_sales_budget (
    sales_budget_id BIGINT IDENTITY(1,1),
    customer_name VARCHAR(255),
    bu_code VARCHAR(50),
    bu_currency VARCHAR(3),
    budget_year INT,
    budget_version VARCHAR(100),
    budget_date DATE,
    sales_amount_bu_currency DECIMAL(18,2),
    pipeline_run_id UNIQUEIDENTIFIER,
    ingestion_timestamp DATETIME2 DEFAULT SYSUTCDATETIME(),
    source_file_name VARCHAR(255)
);

CREATE TABLE [raw_transaction] (
    transaction_nsid BIGINT,
    bu_nsid BIGINT,
    customer_nsid BIGINT,
    transaction_type VARCHAR(100),
    transaction_status VARCHAR(100),
    transaction_number VARCHAR(100),
    transaction_date DATE,
    transaction_last_modified_date DATETIME2,
    expected_delivery_date DATE,
    pipeline_run_id UNIQUEIDENTIFIER,
    ingestion_timestamp DATETIME2 DEFAULT SYSUTCDATETIME(),
    source_file_name VARCHAR(255)
);

CREATE TABLE raw_transactionline (
    transaction_nsid BIGINT,
    item_nsid BIGINT,
    transaction_line_nsid BIGINT,
    quantity DECIMAL(18,4),
    foreign_amount DECIMAL(18,2),
    foreign_currency VARCHAR(3),
    bu_rate DECIMAL(18,8),
    transaction_line_last_modified_date DATETIME2,
    pipeline_run_id UNIQUEIDENTIFIER,
    ingestion_timestamp DATETIME2 DEFAULT SYSUTCDATETIME(),
    source_file_name VARCHAR(255)
);

CREATE TABLE raw_fx_avg_rate (
    original_currency VARCHAR(3),
    target_currency VARCHAR(3),
    rate_date DATE,
    avg_rate DECIMAL(18,8),
    pipeline_run_id UNIQUEIDENTIFIER,
    ingestion_timestamp DATETIME2 DEFAULT SYSUTCDATETIME(),
    source_file_name VARCHAR(255)
);

CREATE TABLE raw_deleted_records (
    transaction_nsid BIGINT,
    deleted_date DATE,
    pipeline_run_id UNIQUEIDENTIFIER,
    ingestion_timestamp DATETIME2 DEFAULT SYSUTCDATETIME(),
    source_file_name VARCHAR(255)
);

CREATE TABLE raw_user_rls (
    user_email VARCHAR(255),
    authorized_bu_code VARCHAR(500),
    authorized_customer_name VARCHAR(255),
    authorized_item_type VARCHAR(255),
    pipeline_run_id UNIQUEIDENTIFIER,
    ingestion_timestamp DATETIME2 DEFAULT SYSUTCDATETIME(),
    source_file_name VARCHAR(255)
);

CREATE TABLE audit_pipeline_run
(
    pipeline_run_id UNIQUEIDENTIFIER PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    start_time DATETIME2 NOT NULL,
    end_time DATETIME2 NULL,
    status VARCHAR(20) NOT NULL,
    error_message VARCHAR(MAX) NULL
);

CREATE TABLE audit_table_load
(
    table_load_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id UNIQUEIDENTIFIER NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    source_file_name VARCHAR(255) NOT NULL,
    source_row_count BIGINT NOT NULL,
    loaded_row_count BIGINT NULL,
    start_time DATETIME2 NOT NULL,
    end_time DATETIME2 NULL,
    status VARCHAR(20) NOT NULL,
    error_message VARCHAR(MAX) NULL
);