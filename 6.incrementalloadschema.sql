/*==============================================================
  CREATE DWH SCHEMA
==============================================================*/

IF NOT EXISTS
(
    SELECT 1
    FROM sys.schemas
    WHERE name = 'dwh'
)
BEGIN
    EXEC('CREATE SCHEMA dwh');
END;
GO


/*==============================================================
  CUSTOMER
==============================================================*/

IF OBJECT_ID('dwh.customer', 'U') IS NULL
BEGIN
    CREATE TABLE dwh.customer
    (
        customer_nsid      BIGINT NOT NULL,
        customer_name      NVARCHAR(255) NULL,
        customer_tier      NVARCHAR(100) NULL,

        row_hash           VARBINARY(32) NOT NULL,
        insert_timestamp   DATETIME2(7) NOT NULL,
        update_timestamp   DATETIME2(7) NOT NULL,
        pipeline_run_id    UNIQUEIDENTIFIER NOT NULL,

        CONSTRAINT PK_dwh_customer
            PRIMARY KEY (customer_nsid)
    );
END;
GO


/*==============================================================
  ITEM CATEGORY
==============================================================*/

IF OBJECT_ID('dwh.item_category', 'U') IS NULL
BEGIN
    CREATE TABLE dwh.item_category
    (
        item_category_nsid BIGINT NOT NULL,
        item_category      NVARCHAR(255) NULL,
        item_sub_category  NVARCHAR(255) NULL,

        row_hash           VARBINARY(32) NOT NULL,
        insert_timestamp   DATETIME2(7) NOT NULL,
        update_timestamp   DATETIME2(7) NOT NULL,
        pipeline_run_id    UNIQUEIDENTIFIER NOT NULL,

        CONSTRAINT PK_dwh_item_category
            PRIMARY KEY (item_category_nsid)
    );
END;
GO


/*==============================================================
  ITEM PATTERN
==============================================================*/

IF OBJECT_ID('dwh.item_pattern', 'U') IS NULL
BEGIN
    CREATE TABLE dwh.item_pattern
    (
        item_pattern_nsid  BIGINT NOT NULL,
        item_pattern       NVARCHAR(255) NULL,

        row_hash           VARBINARY(32) NOT NULL,
        insert_timestamp   DATETIME2(7) NOT NULL,
        update_timestamp   DATETIME2(7) NOT NULL,
        pipeline_run_id    UNIQUEIDENTIFIER NOT NULL,

        CONSTRAINT PK_dwh_item_pattern
            PRIMARY KEY (item_pattern_nsid)
    );
END;
GO


/*==============================================================
  ITEM
==============================================================*/

IF OBJECT_ID('dwh.item', 'U') IS NULL
BEGIN
    CREATE TABLE dwh.item
    (
        item_nsid           BIGINT NOT NULL,
        item_name           NVARCHAR(255) NULL,
        item_code           NVARCHAR(100) NULL,
        item_type           NVARCHAR(100) NULL,
        project_code        NVARCHAR(100) NULL,
        item_category_nsid  BIGINT NULL,
        item_pattern_nsid   BIGINT NULL,

        row_hash            VARBINARY(32) NOT NULL,
        insert_timestamp    DATETIME2(7) NOT NULL,
        update_timestamp    DATETIME2(7) NOT NULL,
        pipeline_run_id     UNIQUEIDENTIFIER NOT NULL,

        CONSTRAINT PK_dwh_item
            PRIMARY KEY (item_nsid)
    );
END;
GO


/*==============================================================
  SUBSIDIARY
==============================================================*/

IF OBJECT_ID('dwh.subsidiary', 'U') IS NULL
BEGIN
    CREATE TABLE dwh.subsidiary
    (
        bu_nsid              BIGINT NOT NULL,
        bu_code              NVARCHAR(50) NULL,
        bu_country_code      NVARCHAR(10) NULL,
        bu_currency          NVARCHAR(10) NULL,
        bu_legal_name        NVARCHAR(255) NULL,
        bu_commercial_group  NVARCHAR(100) NULL,

        row_hash             VARBINARY(32) NOT NULL,
        insert_timestamp     DATETIME2(7) NOT NULL,
        update_timestamp     DATETIME2(7) NOT NULL,
        pipeline_run_id      UNIQUEIDENTIFIER NOT NULL,

        CONSTRAINT PK_dwh_subsidiary
            PRIMARY KEY (bu_nsid)
    );
END;
GO

/*==============================================================
  SALES BUDGET
==============================================================*/

IF OBJECT_ID('dwh.sales_budget', 'U') IS NULL
BEGIN
    CREATE TABLE dwh.sales_budget
    (
        budget_year                 INT NOT NULL,
        budget_version              NVARCHAR(50) NOT NULL,
        budget_date                 DATE NOT NULL,
        customer_name               NVARCHAR(255) NOT NULL,
        bu_code                     NVARCHAR(50) NOT NULL,
        bu_currency                 NVARCHAR(10) NULL,
        sales_amount_bu_currency    DECIMAL(38, 10) NULL,

        row_hash                    VARBINARY(32) NOT NULL,
        insert_timestamp            DATETIME2(7) NOT NULL,
        update_timestamp            DATETIME2(7) NOT NULL,
        pipeline_run_id             UNIQUEIDENTIFIER NOT NULL,

        CONSTRAINT PK_dwh_sales_budget
            PRIMARY KEY
            (
                budget_version,
                budget_date,
                customer_name,
                bu_code
            )
    );
END;
GO


/*==============================================================
  FX AVERAGE RATE
==============================================================*/

IF OBJECT_ID('dwh.fx_avg_rate', 'U') IS NULL
BEGIN
    CREATE TABLE dwh.fx_avg_rate
    (
        original_currency  NVARCHAR(10) NOT NULL,
        target_currency    NVARCHAR(10) NOT NULL,
        rate_date          DATE NOT NULL,
        avg_rate           DECIMAL(38, 18) NOT NULL,

        row_hash           VARBINARY(32) NOT NULL,
        insert_timestamp   DATETIME2(7) NOT NULL,
        update_timestamp   DATETIME2(7) NOT NULL,
        pipeline_run_id    UNIQUEIDENTIFIER NOT NULL,

        CONSTRAINT PK_dwh_fx_avg_rate
            PRIMARY KEY
            (
                original_currency,
                target_currency,
                rate_date
            )
    );
END;
GO


/*==============================================================
  TRANSACTIONS
==============================================================*/

IF OBJECT_ID('dwh.transactions', 'U') IS NULL
BEGIN
    CREATE TABLE dwh.transactions
    (
        transaction_nsid                BIGINT NOT NULL,
        transaction_type                NVARCHAR(100) NULL,
        transaction_status              NVARCHAR(100) NULL,
        transaction_number              NVARCHAR(100) NULL,
        transaction_date                DATE NOT NULL,
        transaction_last_modified_date  DATETIME2(7) NOT NULL,
        expected_delivery_date          DATE NULL,
        bu_nsid                         BIGINT NULL,
        customer_nsid                   BIGINT NULL,

        insert_timestamp                DATETIME2(7) NOT NULL,
        update_timestamp                DATETIME2(7) NOT NULL,
        pipeline_run_id                 UNIQUEIDENTIFIER NOT NULL,

        CONSTRAINT PK_dwh_transactions
            PRIMARY KEY (transaction_nsid)
    );
END;
GO


/*==============================================================
  TRANSACTION LINES
==============================================================*/

IF OBJECT_ID('dwh.transactionline', 'U') IS NULL
BEGIN
    CREATE TABLE dwh.transactionline
    (
        transaction_nsid                       BIGINT NOT NULL,
        transaction_line_nsid                  BIGINT NOT NULL,
        quantity                               DECIMAL(38, 10) NULL,
        foreign_amount                         DECIMAL(38, 10) NULL,
        foreign_currency                       NVARCHAR(10) NULL,
        bu_rate                                DECIMAL(38, 18) NULL,
        item_nsid                              BIGINT NULL,
        transaction_line_last_modified_date    DATETIME2(7) NOT NULL,

        insert_timestamp                       DATETIME2(7) NOT NULL,
        update_timestamp                       DATETIME2(7) NOT NULL,
        pipeline_run_id                        UNIQUEIDENTIFIER NOT NULL,

        CONSTRAINT PK_dwh_transactionline
            PRIMARY KEY
            (
                transaction_nsid,
                transaction_line_nsid
            )
    );
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_dwh_item_category_nsid'
      AND object_id = OBJECT_ID('dwh.item')
)
BEGIN
    CREATE INDEX IX_dwh_item_category_nsid
        ON dwh.item (item_category_nsid);
END;
GO


IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_dwh_item_pattern_nsid'
      AND object_id = OBJECT_ID('dwh.item')
)
BEGIN
    CREATE INDEX IX_dwh_item_pattern_nsid
        ON dwh.item (item_pattern_nsid);
END;
GO


IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_dwh_transactions_modified'
      AND object_id = OBJECT_ID('dwh.transactions')
)
BEGIN
    CREATE INDEX IX_dwh_transactions_modified
        ON dwh.transactions
        (
            transaction_last_modified_date
        );
END;
GO


IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_dwh_transactions_customer'
      AND object_id = OBJECT_ID('dwh.transactions')
)
BEGIN
    CREATE INDEX IX_dwh_transactions_customer
        ON dwh.transactions
        (
            customer_nsid
        );
END;
GO


IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_dwh_transactions_subsidiary'
      AND object_id = OBJECT_ID('dwh.transactions')
)
BEGIN
    CREATE INDEX IX_dwh_transactions_subsidiary
        ON dwh.transactions
        (
            bu_nsid
        );
END;
GO


IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_dwh_transactionline_item'
      AND object_id = OBJECT_ID('dwh.transactionline')
)
BEGIN
    CREATE INDEX IX_dwh_transactionline_item
        ON dwh.transactionline
        (
            item_nsid
        );
END;
GO