/* Returns the latest modification timestamp for each transaction, including its transaction lines. */

CREATE OR ALTER VIEW int.transaction_modified
AS
WITH LineModified AS
(
    SELECT
        transaction_nsid,
        MAX(transaction_line_last_modified_date) AS max_line_modified_date
    FROM stg.transactionline
    GROUP BY transaction_nsid
)
SELECT
    t.transaction_nsid,
    t.transaction_last_modified_date,
    lm.max_line_modified_date,
    CASE
        WHEN lm.max_line_modified_date > t.transaction_last_modified_date
            THEN lm.max_line_modified_date
        ELSE t.transaction_last_modified_date
    END AS transaction_max_modified_date
FROM stg.transactions AS t
LEFT JOIN LineModified AS lm
    ON t.transaction_nsid = lm.transaction_nsid;
GO

/* Returns transaction lines enriched with transaction attributes and the latest transaction modification timestamp. */

CREATE OR ALTER VIEW int.transactionline
AS
SELECT
    tl.transaction_nsid,
    tl.transaction_line_nsid,

    t.transaction_number,
    t.transaction_type,
    t.transaction_status,
    t.transaction_date,
    t.transaction_last_modified_date,
    t.expected_delivery_date,
    t.bu_nsid,
    t.customer_nsid,

    tl.item_nsid,
    tl.quantity,
    tl.foreign_amount,
    tl.foreign_currency,
    tl.bu_rate,
    tl.transaction_line_last_modified_date,

    m.transaction_max_modified_date,

    tl.pipeline_run_id,
    tl.ingestion_timestamp,
    tl.source_file_name

FROM stg.transactionline AS tl
INNER JOIN stg.transactions AS t
    ON tl.transaction_nsid = t.transaction_nsid
INNER JOIN int.transaction_modified AS m
    ON tl.transaction_nsid = m.transaction_nsid;
GO

/* Returns items enriched with their category and pattern attributes. */

CREATE OR ALTER VIEW int.item
AS
SELECT
    i.item_nsid,
    i.item_name,
    i.item_code,
    i.item_type,
    i.project_code,

    i.item_category_nsid,
    c.item_category,
    c.item_sub_category,

    i.item_pattern_nsid,
    p.item_pattern,

    i.pipeline_run_id,
    i.ingestion_timestamp,
    i.source_file_name

FROM stg.item AS i
LEFT JOIN stg.item_category AS c
    ON i.item_category_nsid = c.item_category_nsid
LEFT JOIN stg.item_pattern AS p
    ON i.item_pattern_nsid = p.item_pattern_nsid;
GO

/* Returns FX rates with the next effective rate date for each currency pair. */

CREATE OR ALTER VIEW int.fx_avg_rate
AS
SELECT
    original_currency,
    target_currency,
    rate_date,
    avg_rate,

    LEAD(rate_date) OVER
    (
        PARTITION BY
            original_currency,
            target_currency
        ORDER BY rate_date
    ) AS next_rate_date,

    pipeline_run_id,
    ingestion_timestamp,
    source_file_name

FROM stg.fx_avg_rate;
GO

CREATE OR ALTER VIEW int.sales_budget
AS
SELECT
    b.budget_year,
    b.budget_version,
    b.budget_date,
    b.customer_name,

    b.bu_code,
    s.bu_nsid,
    b.bu_currency,

    b.sales_amount_bu_currency,

    b.pipeline_run_id,
    b.ingestion_timestamp,
    b.source_file_name

FROM stg.sales_budget AS b
LEFT JOIN stg.subsidiary AS s
    ON b.bu_code = s.bu_code;
GO

/* Returns deleted transaction records from the source system. */

CREATE OR ALTER VIEW int.deleted_transaction
AS
SELECT
    transaction_nsid,
    deleted_date,
    pipeline_run_id,
    ingestion_timestamp,
    source_file_name
FROM stg.deleted_records;
GO

/* Returns transaction lines enriched with customer, subsidiary, item, and calculated business-unit amount. */

CREATE OR ALTER VIEW int.transaction_enriched
AS
SELECT
    tl.transaction_nsid,
    tl.transaction_line_nsid,

    tl.transaction_number,
    tl.transaction_type,
    tl.transaction_status,
    tl.transaction_date,
    tl.expected_delivery_date,
    tl.transaction_max_modified_date,

    tl.customer_nsid,
    c.customer_name,
    c.customer_tier,

    tl.bu_nsid,
    s.bu_code,

    tl.item_nsid,
    i.item_name,
    i.item_code,
    i.item_type,
    i.project_code,
    i.item_category,
    i.item_sub_category,
    i.item_pattern,

    tl.quantity,
    tl.foreign_amount,
    tl.foreign_currency,
    tl.bu_rate,

    tl.foreign_amount * tl.bu_rate
        AS amount_bu_currency,

    tl.pipeline_run_id,
    tl.ingestion_timestamp,
    tl.source_file_name

FROM int.transactionline AS tl
LEFT JOIN stg.customer AS c
    ON tl.customer_nsid = c.customer_nsid
LEFT JOIN stg.subsidiary AS s
    ON tl.bu_nsid = s.bu_nsid
LEFT JOIN int.item AS i
    ON tl.item_nsid = i.item_nsid;
GO