CREATE TABLE audit.watermark
(
    watermark_id BIGINT IDENTITY(1,1) NOT NULL,
    process_name VARCHAR(255) NOT NULL,
    last_watermark DATETIME2(7) NULL,
    last_pipeline_run_id UNIQUEIDENTIFIER NULL,
    last_run_timestamp DATETIME2(7) NULL,
    status VARCHAR(20) NOT NULL,

    CONSTRAINT PK_watermark
        PRIMARY KEY (watermark_id),

    CONSTRAINT UQ_watermark_process
        UNIQUE (process_name),

    CONSTRAINT CK_watermark_status
        CHECK (status IN ('READY', 'RUNNING', 'SUCCESS', 'FAILED'))
);
GO

CREATE OR ALTER PROCEDURE dwh.usp_load_customer
    @pipeline_run_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        WITH LatestCustomer AS
        (
            SELECT
                customer_nsid,
                customer_name,
                customer_tier,

                HASHBYTES(
                    'SHA2_256',
                    CONCAT(
                        'customer_name=',
                        COALESCE(
                            UPPER(LTRIM(RTRIM(customer_name))),
                            ''
                        ),
                        '|customer_tier=',
                        COALESCE(
                            UPPER(LTRIM(RTRIM(customer_tier))),
                            ''
                        )
                    )
                ) AS row_hash,

                ROW_NUMBER() OVER
                (
                    PARTITION BY customer_nsid
                    ORDER BY
                        ingestion_timestamp DESC,
                        pipeline_run_id DESC
                ) AS row_number

            FROM stg.customer
        )

        MERGE INTO dwh.customer AS target
        USING
        (
            SELECT
                customer_nsid,
                customer_name,
                customer_tier,
                row_hash
            FROM LatestCustomer
            WHERE row_number = 1
        ) AS source
            ON target.customer_nsid = source.customer_nsid

        WHEN MATCHED
             AND target.row_hash <> source.row_hash
        THEN
            UPDATE SET
                target.customer_name = source.customer_name,
                target.customer_tier = source.customer_tier,
                target.row_hash = source.row_hash,
                target.update_timestamp = SYSUTCDATETIME(),
                target.pipeline_run_id = @pipeline_run_id

        WHEN NOT MATCHED BY TARGET
        THEN
            INSERT
            (
                customer_nsid,
                customer_name,
                customer_tier,
                row_hash,
                insert_timestamp,
                update_timestamp,
                pipeline_run_id
            )
            VALUES
            (
                source.customer_nsid,
                source.customer_name,
                source.customer_tier,
                source.row_hash,
                SYSUTCDATETIME(),
                SYSUTCDATETIME(),
                @pipeline_run_id
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

CREATE OR ALTER PROCEDURE dwh.usp_load_item_category
    @pipeline_run_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        ;WITH LatestSource AS
        (
            SELECT
                item_category_nsid,
                item_category,
                item_sub_category,

                HASHBYTES
                (
                    'SHA2_256',
                    CONCAT
                    (
                        'item_category=',
                        COALESCE(
                            UPPER(LTRIM(RTRIM(item_category))),
                            '<NULL>'
                        ),
                        '|item_sub_category=',
                        COALESCE(
                            UPPER(LTRIM(RTRIM(item_sub_category))),
                            '<NULL>'
                        )
                    )
                ) AS row_hash,

                ROW_NUMBER() OVER
                (
                    PARTITION BY item_category_nsid
                    ORDER BY
                        ingestion_timestamp DESC,
                        pipeline_run_id DESC
                ) AS row_number

            FROM stg.item_category
            WHERE item_category_nsid IS NOT NULL
        )

        MERGE dwh.item_category AS target
        USING
        (
            SELECT
                item_category_nsid,
                item_category,
                item_sub_category,
                row_hash
            FROM LatestSource
            WHERE row_number = 1
        ) AS source
            ON target.item_category_nsid =
               source.item_category_nsid

        WHEN MATCHED
             AND target.row_hash <> source.row_hash
        THEN
            UPDATE SET
                target.item_category =
                    source.item_category,
                target.item_sub_category =
                    source.item_sub_category,
                target.row_hash =
                    source.row_hash,
                target.update_timestamp =
                    SYSUTCDATETIME(),
                target.pipeline_run_id =
                    @pipeline_run_id

        WHEN NOT MATCHED BY TARGET
        THEN
            INSERT
            (
                item_category_nsid,
                item_category,
                item_sub_category,
                row_hash,
                insert_timestamp,
                update_timestamp,
                pipeline_run_id
            )
            VALUES
            (
                source.item_category_nsid,
                source.item_category,
                source.item_sub_category,
                source.row_hash,
                SYSUTCDATETIME(),
                SYSUTCDATETIME(),
                @pipeline_run_id
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

CREATE OR ALTER PROCEDURE dwh.usp_load_item_pattern
    @pipeline_run_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        ;WITH LatestSource AS
        (
            SELECT
                item_pattern_nsid,
                item_pattern,

                HASHBYTES
                (
                    'SHA2_256',
                    CONCAT
                    (
                        'item_pattern=',
                        COALESCE(
                            UPPER(LTRIM(RTRIM(item_pattern))),
                            '<NULL>'
                        )
                    )
                ) AS row_hash,

                ROW_NUMBER() OVER
                (
                    PARTITION BY item_pattern_nsid
                    ORDER BY
                        ingestion_timestamp DESC,
                        pipeline_run_id DESC
                ) AS row_number

            FROM stg.item_pattern
            WHERE item_pattern_nsid IS NOT NULL
        )

        MERGE dwh.item_pattern AS target
        USING
        (
            SELECT
                item_pattern_nsid,
                item_pattern,
                row_hash
            FROM LatestSource
            WHERE row_number = 1
        ) AS source
            ON target.item_pattern_nsid =
               source.item_pattern_nsid

        WHEN MATCHED
             AND target.row_hash <> source.row_hash
        THEN
            UPDATE SET
                target.item_pattern =
                    source.item_pattern,
                target.row_hash =
                    source.row_hash,
                target.update_timestamp =
                    SYSUTCDATETIME(),
                target.pipeline_run_id =
                    @pipeline_run_id

        WHEN NOT MATCHED BY TARGET
        THEN
            INSERT
            (
                item_pattern_nsid,
                item_pattern,
                row_hash,
                insert_timestamp,
                update_timestamp,
                pipeline_run_id
            )
            VALUES
            (
                source.item_pattern_nsid,
                source.item_pattern,
                source.row_hash,
                SYSUTCDATETIME(),
                SYSUTCDATETIME(),
                @pipeline_run_id
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

CREATE OR ALTER PROCEDURE dwh.usp_load_item
    @pipeline_run_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        ;WITH LatestSource AS
        (
            SELECT
                item_nsid,
                item_name,
                item_code,
                item_type,
                project_code,
                item_category_nsid,
                item_pattern_nsid,

                HASHBYTES
                (
                    'SHA2_256',
                    CONCAT
                    (
                        'item_name=',
                        COALESCE(
                            UPPER(LTRIM(RTRIM(item_name))),
                            '<NULL>'
                        ),
                        '|item_code=',
                        COALESCE(
                            UPPER(LTRIM(RTRIM(item_code))),
                            '<NULL>'
                        ),
                        '|item_type=',
                        COALESCE(
                            UPPER(LTRIM(RTRIM(item_type))),
                            '<NULL>'
                        ),
                        '|project_code=',
                        COALESCE(
                            UPPER(LTRIM(RTRIM(project_code))),
                            '<NULL>'
                        ),
                        '|item_category_nsid=',
                        COALESCE(
                            CONVERT(
                                VARCHAR(30),
                                item_category_nsid
                            ),
                            '<NULL>'
                        ),
                        '|item_pattern_nsid=',
                        COALESCE(
                            CONVERT(
                                VARCHAR(30),
                                item_pattern_nsid
                            ),
                            '<NULL>'
                        )
                    )
                ) AS row_hash,

                ROW_NUMBER() OVER
                (
                    PARTITION BY item_nsid
                    ORDER BY
                        ingestion_timestamp DESC,
                        pipeline_run_id DESC
                ) AS row_number

            FROM stg.item
            WHERE item_nsid IS NOT NULL
        )

        MERGE dwh.item AS target
        USING
        (
            SELECT
                item_nsid,
                item_name,
                item_code,
                item_type,
                project_code,
                item_category_nsid,
                item_pattern_nsid,
                row_hash
            FROM LatestSource
            WHERE row_number = 1
        ) AS source
            ON target.item_nsid = source.item_nsid

        WHEN MATCHED
             AND target.row_hash <> source.row_hash
        THEN
            UPDATE SET
                target.item_name =
                    source.item_name,
                target.item_code =
                    source.item_code,
                target.item_type =
                    source.item_type,
                target.project_code =
                    source.project_code,
                target.item_category_nsid =
                    source.item_category_nsid,
                target.item_pattern_nsid =
                    source.item_pattern_nsid,
                target.row_hash =
                    source.row_hash,
                target.update_timestamp =
                    SYSUTCDATETIME(),
                target.pipeline_run_id =
                    @pipeline_run_id

        WHEN NOT MATCHED BY TARGET
        THEN
            INSERT
            (
                item_nsid,
                item_name,
                item_code,
                item_type,
                project_code,
                item_category_nsid,
                item_pattern_nsid,
                row_hash,
                insert_timestamp,
                update_timestamp,
                pipeline_run_id
            )
            VALUES
            (
                source.item_nsid,
                source.item_name,
                source.item_code,
                source.item_type,
                source.project_code,
                source.item_category_nsid,
                source.item_pattern_nsid,
                source.row_hash,
                SYSUTCDATETIME(),
                SYSUTCDATETIME(),
                @pipeline_run_id
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

CREATE OR ALTER PROCEDURE dwh.usp_load_subsidiary
    @pipeline_run_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        ;WITH LatestSource AS
        (
            SELECT
                bu_nsid,
                bu_code,
                bu_country_code,
                bu_currency,
                bu_legal_name,
                bu_commercial_group,

                HASHBYTES
                (
                    'SHA2_256',
                    CONCAT
                    (
                        'bu_code=',
                        COALESCE(
                            UPPER(LTRIM(RTRIM(bu_code))),
                            '<NULL>'
                        ),
                        '|bu_country_code=',
                        COALESCE(
                            UPPER(LTRIM(RTRIM(bu_country_code))),
                            '<NULL>'
                        ),
                        '|bu_currency=',
                        COALESCE(
                            UPPER(LTRIM(RTRIM(bu_currency))),
                            '<NULL>'
                        ),
                        '|bu_legal_name=',
                        COALESCE(
                            UPPER(LTRIM(RTRIM(bu_legal_name))),
                            '<NULL>'
                        ),
                        '|bu_commercial_group=',
                        COALESCE(
                            UPPER(
                                LTRIM(
                                    RTRIM(bu_commercial_group)
                                )
                            ),
                            '<NULL>'
                        )
                    )
                ) AS row_hash,

                ROW_NUMBER() OVER
                (
                    PARTITION BY bu_nsid
                    ORDER BY
                        ingestion_timestamp DESC,
                        pipeline_run_id DESC
                ) AS row_number

            FROM stg.subsidiary
            WHERE bu_nsid IS NOT NULL
        )

        MERGE dwh.subsidiary AS target
        USING
        (
            SELECT
                bu_nsid,
                bu_code,
                bu_country_code,
                bu_currency,
                bu_legal_name,
                bu_commercial_group,
                row_hash
            FROM LatestSource
            WHERE row_number = 1
        ) AS source
            ON target.bu_nsid = source.bu_nsid

        WHEN MATCHED
             AND target.row_hash <> source.row_hash
        THEN
            UPDATE SET
                target.bu_code =
                    source.bu_code,
                target.bu_country_code =
                    source.bu_country_code,
                target.bu_currency =
                    source.bu_currency,
                target.bu_legal_name =
                    source.bu_legal_name,
                target.bu_commercial_group =
                    source.bu_commercial_group,
                target.row_hash =
                    source.row_hash,
                target.update_timestamp =
                    SYSUTCDATETIME(),
                target.pipeline_run_id =
                    @pipeline_run_id

        WHEN NOT MATCHED BY TARGET
        THEN
            INSERT
            (
                bu_nsid,
                bu_code,
                bu_country_code,
                bu_currency,
                bu_legal_name,
                bu_commercial_group,
                row_hash,
                insert_timestamp,
                update_timestamp,
                pipeline_run_id
            )
            VALUES
            (
                source.bu_nsid,
                source.bu_code,
                source.bu_country_code,
                source.bu_currency,
                source.bu_legal_name,
                source.bu_commercial_group,
                source.row_hash,
                SYSUTCDATETIME(),
                SYSUTCDATETIME(),
                @pipeline_run_id
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

CREATE OR ALTER PROCEDURE dwh.usp_load_sales_budget
    @pipeline_run_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        ;WITH LatestSource AS
        (
            SELECT
                budget_year,
                budget_version,
                budget_date,
                customer_name,
                bu_code,
                bu_currency,
                sales_amount_bu_currency,

                HASHBYTES
                (
                    'SHA2_256',
                    CONCAT
                    (
                        'budget_year=',
                        CONVERT(
                            VARCHAR(10),
                            budget_year
                        ),
                        '|bu_currency=',
                        COALESCE(
                            UPPER(LTRIM(RTRIM(bu_currency))),
                            '<NULL>'
                        ),
                        '|sales_amount=',
                        COALESCE(
                            CONVERT(
                                VARCHAR(100),
                                sales_amount_bu_currency
                            ),
                            '<NULL>'
                        )
                    )
                ) AS row_hash,

                ROW_NUMBER() OVER
                (
                    PARTITION BY
                        budget_version,
                        budget_date,
                        customer_name,
                        bu_code
                    ORDER BY
                        ingestion_timestamp DESC,
                        pipeline_run_id DESC
                ) AS row_number

            FROM stg.sales_budget
            WHERE budget_version IS NOT NULL
              AND budget_date IS NOT NULL
              AND customer_name IS NOT NULL
              AND bu_code IS NOT NULL
        )

        MERGE dwh.sales_budget AS target
        USING
        (
            SELECT
                budget_year,
                budget_version,
                budget_date,
                customer_name,
                bu_code,
                bu_currency,
                sales_amount_bu_currency,
                row_hash
            FROM LatestSource
            WHERE row_number = 1
        ) AS source
            ON target.budget_version =
               source.budget_version
           AND target.budget_date =
               source.budget_date
           AND target.customer_name =
               source.customer_name
           AND target.bu_code =
               source.bu_code

        WHEN MATCHED
             AND target.row_hash <> source.row_hash
        THEN
            UPDATE SET
                target.budget_year =
                    source.budget_year,
                target.bu_currency =
                    source.bu_currency,
                target.sales_amount_bu_currency =
                    source.sales_amount_bu_currency,
                target.row_hash =
                    source.row_hash,
                target.update_timestamp =
                    SYSUTCDATETIME(),
                target.pipeline_run_id =
                    @pipeline_run_id

        WHEN NOT MATCHED BY TARGET
        THEN
            INSERT
            (
                budget_year,
                budget_version,
                budget_date,
                customer_name,
                bu_code,
                bu_currency,
                sales_amount_bu_currency,
                row_hash,
                insert_timestamp,
                update_timestamp,
                pipeline_run_id
            )
            VALUES
            (
                source.budget_year,
                source.budget_version,
                source.budget_date,
                source.customer_name,
                source.bu_code,
                source.bu_currency,
                source.sales_amount_bu_currency,
                source.row_hash,
                SYSUTCDATETIME(),
                SYSUTCDATETIME(),
                @pipeline_run_id
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

CREATE OR ALTER PROCEDURE dwh.usp_load_fx_avg_rate
    @pipeline_run_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        ;WITH LatestSource AS
        (
            SELECT
                original_currency,
                target_currency,
                rate_date,
                avg_rate,

                HASHBYTES
                (
                    'SHA2_256',
                    CONCAT
                    (
                        'avg_rate=',
                        CONVERT(
                            VARCHAR(100),
                            avg_rate
                        )
                    )
                ) AS row_hash,

                ROW_NUMBER() OVER
                (
                    PARTITION BY
                        original_currency,
                        target_currency,
                        rate_date
                    ORDER BY
                        ingestion_timestamp DESC,
                        pipeline_run_id DESC
                ) AS row_number

            FROM stg.fx_avg_rate
            WHERE original_currency IS NOT NULL
              AND target_currency IS NOT NULL
              AND rate_date IS NOT NULL
        )

        MERGE dwh.fx_avg_rate AS target
        USING
        (
            SELECT
                original_currency,
                target_currency,
                rate_date,
                avg_rate,
                row_hash
            FROM LatestSource
            WHERE row_number = 1
        ) AS source
            ON target.original_currency =
               source.original_currency
           AND target.target_currency =
               source.target_currency
           AND target.rate_date =
               source.rate_date

        WHEN MATCHED
             AND target.row_hash <> source.row_hash
        THEN
            UPDATE SET
                target.avg_rate =
                    source.avg_rate,
                target.row_hash =
                    source.row_hash,
                target.update_timestamp =
                    SYSUTCDATETIME(),
                target.pipeline_run_id =
                    @pipeline_run_id

        WHEN NOT MATCHED BY TARGET
        THEN
            INSERT
            (
                original_currency,
                target_currency,
                rate_date,
                avg_rate,
                row_hash,
                insert_timestamp,
                update_timestamp,
                pipeline_run_id
            )
            VALUES
            (
                source.original_currency,
                source.target_currency,
                source.rate_date,
                source.avg_rate,
                source.row_hash,
                SYSUTCDATETIME(),
                SYSUTCDATETIME(),
                @pipeline_run_id
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

CREATE OR ALTER PROCEDURE dwh.usp_load_transactions
    @pipeline_run_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @process_name VARCHAR(255) =
            'DWH_TRANSACTION_LOAD',
        @last_watermark DATETIME2(7),
        @new_watermark DATETIME2(7);

    BEGIN TRY
        BEGIN TRANSACTION;

        SELECT
            @last_watermark =
                COALESCE(last_watermark, '19000101')
        FROM audit.watermark WITH
        (
            UPDLOCK,
            HOLDLOCK
        )
        WHERE process_name = @process_name;

        IF @last_watermark IS NULL
        BEGIN
            THROW 50001,
                'Transaction watermark was not found.',
                1;
        END;

        UPDATE audit.watermark
        SET
            status = 'RUNNING',
            last_pipeline_run_id =
                @pipeline_run_id,
            last_run_timestamp =
                SYSUTCDATETIME()
        WHERE process_name = @process_name;

        /*
            The new watermark includes both modified transactions
            and transaction deletion dates.
        */
        SELECT
            @new_watermark = MAX(change_timestamp)
        FROM
        (
            SELECT
                transaction_max_modified_date
                    AS change_timestamp
            FROM int.transaction_modified

            UNION ALL

            SELECT
                CAST(deleted_date AS DATETIME2(7))
                    AS change_timestamp
            FROM int.deleted_transaction
        ) AS changes;

        SET @new_watermark =
            COALESCE(
                @new_watermark,
                @last_watermark
            );

        CREATE TABLE #AffectedTransactions
        (
            transaction_nsid BIGINT NOT NULL,

            CONSTRAINT PK_AffectedTransactions
                PRIMARY KEY (transaction_nsid)
        );

        INSERT INTO #AffectedTransactions
        (
            transaction_nsid
        )
        SELECT DISTINCT
            modified.transaction_nsid
        FROM int.transaction_modified AS modified
        WHERE modified.transaction_max_modified_date
                  > @last_watermark
          AND modified.transaction_max_modified_date
                  <= @new_watermark

        UNION

        SELECT DISTINCT
            deleted.transaction_nsid
        FROM int.deleted_transaction AS deleted
        WHERE deleted.deleted_date
                  > @last_watermark
          AND deleted.deleted_date
                  <= @new_watermark;

        /*
            Delete child records first.
        */
        DELETE target
        FROM dwh.transactionline AS target
        INNER JOIN #AffectedTransactions AS affected
            ON target.transaction_nsid =
               affected.transaction_nsid;

        /*
            Then delete the transaction headers.
        */
        DELETE target
        FROM dwh.transactions AS target
        INNER JOIN #AffectedTransactions AS affected
            ON target.transaction_nsid =
               affected.transaction_nsid;

        /*
            Reinsert current transaction headers.
            Permanently deleted transactions are excluded.
        */
        ;WITH LatestTransactions AS
        (
            SELECT
                source.transaction_nsid,
                source.transaction_type,
                source.transaction_status,
                source.transaction_number,
                source.transaction_date,
                source.transaction_last_modified_date,
                source.expected_delivery_date,
                source.bu_nsid,
                source.customer_nsid,

                ROW_NUMBER() OVER
                (
                    PARTITION BY source.transaction_nsid
                    ORDER BY
                        source.ingestion_timestamp DESC,
                        source.pipeline_run_id DESC
                ) AS row_number

            FROM stg.transactions AS source
            INNER JOIN #AffectedTransactions AS affected
                ON source.transaction_nsid =
                   affected.transaction_nsid

            WHERE NOT EXISTS
            (
                SELECT 1
                FROM int.deleted_transaction AS deleted
                WHERE deleted.transaction_nsid =
                      source.transaction_nsid
            )
        )

        INSERT INTO dwh.transactions
        (
            transaction_nsid,
            transaction_type,
            transaction_status,
            transaction_number,
            transaction_date,
            transaction_last_modified_date,
            expected_delivery_date,
            bu_nsid,
            customer_nsid,
            insert_timestamp,
            update_timestamp,
            pipeline_run_id
        )
        SELECT
            transaction_nsid,
            transaction_type,
            transaction_status,
            transaction_number,
            transaction_date,
            transaction_last_modified_date,
            expected_delivery_date,
            bu_nsid,
            customer_nsid,
            SYSUTCDATETIME(),
            SYSUTCDATETIME(),
            @pipeline_run_id
        FROM LatestTransactions
        WHERE row_number = 1;

        /*
            Reinsert every current line belonging to each
            affected transaction.
        */
        ;WITH LatestTransactionLines AS
        (
            SELECT
                source.transaction_nsid,
                source.transaction_line_nsid,
                source.quantity,
                source.foreign_amount,
                source.foreign_currency,
                source.bu_rate,
                source.item_nsid,
                source.transaction_line_last_modified_date,

                ROW_NUMBER() OVER
                (
                    PARTITION BY
                        source.transaction_nsid,
                        source.transaction_line_nsid
                    ORDER BY
                        source.ingestion_timestamp DESC,
                        source.pipeline_run_id DESC
                ) AS row_number

            FROM stg.transactionline AS source
            INNER JOIN #AffectedTransactions AS affected
                ON source.transaction_nsid =
                   affected.transaction_nsid

            WHERE NOT EXISTS
            (
                SELECT 1
                FROM int.deleted_transaction AS deleted
                WHERE deleted.transaction_nsid =
                      source.transaction_nsid
            )
        )

        INSERT INTO dwh.transactionline
        (
            transaction_nsid,
            transaction_line_nsid,
            quantity,
            foreign_amount,
            foreign_currency,
            bu_rate,
            item_nsid,
            transaction_line_last_modified_date,
            insert_timestamp,
            update_timestamp,
            pipeline_run_id
        )
        SELECT
            source.transaction_nsid,
            source.transaction_line_nsid,
            source.quantity,
            source.foreign_amount,
            source.foreign_currency,
            source.bu_rate,
            source.item_nsid,
            source.transaction_line_last_modified_date,
            SYSUTCDATETIME(),
            SYSUTCDATETIME(),
            @pipeline_run_id
        FROM LatestTransactionLines AS source
        INNER JOIN dwh.transactions AS header
            ON source.transaction_nsid =
               header.transaction_nsid
        WHERE source.row_number = 1;

        /*
            Only advance the watermark after both tables load.
        */
        UPDATE audit.watermark
        SET
            last_watermark =
                @new_watermark,
            last_pipeline_run_id =
                @pipeline_run_id,
            last_run_timestamp =
                SYSUTCDATETIME(),
            status = 'SUCCESS'
        WHERE process_name = @process_name;

        COMMIT TRANSACTION;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        UPDATE audit.watermark
        SET
            last_pipeline_run_id =
                @pipeline_run_id,
            last_run_timestamp =
                SYSUTCDATETIME(),
            status = 'FAILED'
        WHERE process_name =
              'DWH_TRANSACTION_LOAD';

        THROW;
    END CATCH;
END;
GO