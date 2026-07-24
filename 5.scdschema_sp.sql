CREATE TABLE scd.customer
(
    customer_key BIGINT IDENTITY(1,1) NOT NULL,

    customer_nsid BIGINT NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    customer_tier VARCHAR(100) NULL,

    row_hash VARCHAR(64) NOT NULL,

    effective_from DATETIME2 NOT NULL,
    effective_to DATETIME2 NOT NULL,
    is_current BIT NOT NULL,

    pipeline_run_id UNIQUEIDENTIFIER NOT NULL,
    created_timestamp DATETIME2 NOT NULL,

    CONSTRAINT PK_customer
        PRIMARY KEY (customer_key),

    CONSTRAINT CK_customer_dates
        CHECK (effective_from < effective_to),

    CONSTRAINT CK_customer_current
        CHECK (is_current IN (0, 1))
);
GO

CREATE UNIQUE INDEX UX_customer_current
ON scd.customer (customer_nsid)
WHERE is_current = 1;
GO

CREATE OR ALTER PROCEDURE scd.usp_load_customer
    @pipeline_run_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @load_timestamp DATETIME2(7) = SYSUTCDATETIME();
    DECLARE @future_timestamp DATETIME2(7) =
        '9999-12-31 23:59:59.9999999';

    BEGIN TRY
        BEGIN TRANSACTION;

        DROP TABLE IF EXISTS #source_customer;

        SELECT
            customer_nsid,
            customer_name,
            customer_tier,

            CONVERT(
                CHAR(64),
                HASHBYTES(
                    'SHA2_256',
                    CONCAT(
                        COALESCE(customer_name, ''),
                        '|',
                        COALESCE(customer_tier, '')
                    )
                ),
                2
            ) AS row_hash

        INTO #source_customer

        FROM stg.customer;


        /* Expire current versions that have changed */
        UPDATE target
        SET
            target.effective_to = @load_timestamp,
            target.is_current = 0
        FROM scd.customer AS target
        INNER JOIN #source_customer AS source
            ON target.customer_nsid = source.customer_nsid
        WHERE target.is_current = 1
          AND target.row_hash <> source.row_hash;


        /* Insert new customers and new changed versions */
        INSERT INTO scd.customer
        (
            customer_nsid,
            customer_name,
            customer_tier,
            row_hash,
            effective_from,
            effective_to,
            is_current,
            pipeline_run_id,
            created_timestamp
        )
        SELECT
            source.customer_nsid,
            source.customer_name,
            source.customer_tier,
            source.row_hash,
            @load_timestamp,
            @future_timestamp,
            1,
            @pipeline_run_id,
            @load_timestamp
        FROM #source_customer AS source
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM scd.customer AS target
            WHERE target.customer_nsid = source.customer_nsid
              AND target.is_current = 1
              AND target.row_hash = source.row_hash
        );

        COMMIT TRANSACTION;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH;
END;
GO

CREATE TABLE scd.item
(
    item_key BIGINT IDENTITY(1,1) NOT NULL,

    item_nsid BIGINT NOT NULL,
    item_name VARCHAR(255) NOT NULL,
    item_code VARCHAR(100) NOT NULL,
    item_type VARCHAR(100) NOT NULL,
    project_code VARCHAR(100) NULL,

    item_category_nsid BIGINT NOT NULL,
    item_pattern_nsid BIGINT NOT NULL,

    row_hash CHAR(64) NOT NULL,

    effective_from DATETIME2(7) NOT NULL,
    effective_to DATETIME2(7) NOT NULL,
    is_current BIT NOT NULL,

    pipeline_run_id UNIQUEIDENTIFIER NOT NULL,
    created_timestamp DATETIME2(7) NOT NULL,

    CONSTRAINT PK_item
        PRIMARY KEY (item_key),

    CONSTRAINT CK_item_dates
        CHECK (effective_from < effective_to),

    CONSTRAINT CK_item_current
        CHECK (is_current IN (0, 1))
);
GO

CREATE UNIQUE INDEX UX_item_current
ON scd.item (item_nsid)
WHERE is_current = 1;
GO

CREATE UNIQUE INDEX UX_item_current
ON scd.item (item_nsid)
WHERE is_current = 1;
GO

CREATE OR ALTER PROCEDURE scd.usp_load_item
    @pipeline_run_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @load_timestamp DATETIME2(7) = SYSUTCDATETIME();
    DECLARE @future_timestamp DATETIME2(7) =
        '9999-12-31 23:59:59.9999999';

    BEGIN TRY
        BEGIN TRANSACTION;

        DROP TABLE IF EXISTS #source_item;

        SELECT
            item_nsid,
            item_name,
            item_code,
            item_type,
            project_code,
            item_category_nsid,
            item_pattern_nsid,

            CONVERT(
                CHAR(64),
                HASHBYTES(
                    'SHA2_256',
                    CONCAT(
                        COALESCE(item_name, ''),
                        '|',
                        COALESCE(item_code, ''),
                        '|',
                        COALESCE(item_type, ''),
                        '|',
                        COALESCE(project_code, ''),
                        '|',
                        COALESCE(
                            CONVERT(VARCHAR(50), item_category_nsid),
                            ''
                        ),
                        '|',
                        COALESCE(
                            CONVERT(VARCHAR(50), item_pattern_nsid),
                            ''
                        )
                    )
                ),
                2
            ) AS row_hash

        INTO #source_item
        FROM stg.item;


        /* Expire changed current versions */
        UPDATE target
        SET
            target.effective_to = @load_timestamp,
            target.is_current = 0
        FROM scd.item AS target
        INNER JOIN #source_item AS source
            ON target.item_nsid = source.item_nsid
        WHERE target.is_current = 1
          AND target.row_hash <> source.row_hash;


        /* Insert new items and new versions */
        INSERT INTO scd.item
        (
            item_nsid,
            item_name,
            item_code,
            item_type,
            project_code,
            item_category_nsid,
            item_pattern_nsid,
            row_hash,
            effective_from,
            effective_to,
            is_current,
            pipeline_run_id,
            created_timestamp
        )
        SELECT
            source.item_nsid,
            source.item_name,
            source.item_code,
            source.item_type,
            source.project_code,
            source.item_category_nsid,
            source.item_pattern_nsid,
            source.row_hash,
            @load_timestamp,
            @future_timestamp,
            1,
            @pipeline_run_id,
            @load_timestamp
        FROM #source_item AS source
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM scd.item AS target
            WHERE target.item_nsid = source.item_nsid
              AND target.is_current = 1
              AND target.row_hash = source.row_hash
        );

        COMMIT TRANSACTION;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH;
END;
GO