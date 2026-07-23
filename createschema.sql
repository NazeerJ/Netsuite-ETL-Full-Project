CREATE TABLE customer (
    customer_nsid BIGINT,
    customer_name VARCHAR(255),
    customer_tier VARCHAR(100)
);

CREATE TABLE item_category (
    item_category_nsid BIGINT,
    item_category VARCHAR(255),
    item_sub_category VARCHAR(255)
);

CREATE TABLE item_pattern (
    item_pattern_nsid BIGINT,
    item_pattern VARCHAR(255)
);

CREATE TABLE item (
    item_nsid BIGINT,
    item_category_nsid BIGINT,
    item_pattern_nsid BIGINT,
    item_name VARCHAR(255),
    item_code VARCHAR(100),
    item_type VARCHAR(100),
    project_code VARCHAR(100)
);

CREATE TABLE subsidiary (
    bu_nsid BIGINT,
    bu_code VARCHAR(50),
    bu_currency VARCHAR(3),
    bu_country_code VARCHAR(10),
    bu_legal_name VARCHAR(255),
    bu_commercial_group VARCHAR(255)
);

CREATE TABLE sales_budget (
    sales_budget_id BIGINT IDENTITY(1,1),
    customer_name VARCHAR(255),
    bu_code VARCHAR(50),
    bu_currency VARCHAR(3),
    budget_year INT,
    budget_version VARCHAR(100),
    budget_date DATE,
    sales_amount_bu_currency DECIMAL(18,2)
);

CREATE TABLE [transaction] (
    transaction_nsid BIGINT,
    bu_nsid BIGINT,
    customer_nsid BIGINT,
    transaction_type VARCHAR(100),
    transaction_status VARCHAR(100),
    transaction_number VARCHAR(100),
    transaction_date DATE,
    transaction_last_modified_date DATETIME2,
    expected_delivery_date DATE
);

CREATE TABLE transactionline (
    transaction_nsid BIGINT,
    item_nsid BIGINT,
    transaction_line_nsid BIGINT,
    quantity DECIMAL(18,4),
    foreign_amount DECIMAL(18,2),
    foreign_currency VARCHAR(3),
    bu_rate DECIMAL(18,8),
    transaction_line_last_modified_date DATETIME2
);

CREATE TABLE fx_avg_rate (
    original_currency VARCHAR(3),
    target_currency VARCHAR(3),
    rate_date DATE,
    avg_rate DECIMAL(18,8)
);

CREATE TABLE deleted_records (
    transaction_nsid BIGINT,
    deleted_date DATE,
    pipeline_run_id UNIQUEIDENTIFIER,
    ingestion_timestamp DATETIME2 DEFAULT SYSUTCDATETIME(),
    source_file_name VARCHAR(255)
);

CREATE TABLE user_rls (
    user_email VARCHAR(255),
    authorized_bu_code VARCHAR(500),
    authorized_customer_name VARCHAR(255),
    authorized_item_type VARCHAR(255),
    pipeline_run_id UNIQUEIDENTIFIER,
    ingestion_timestamp DATETIME2 DEFAULT SYSUTCDATETIME(),
    source_file_name VARCHAR(255)
);