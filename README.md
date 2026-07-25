# NetSuite CSV-to-SQL Server ETL Project

This project demonstrates a complete local ETL pipeline built with **Python, Polars, SQLAlchemy and SQL Server**.

The goal is to take NetSuite-style CSV extracts, validate and transform them, preserve important history, load a current-state warehouse, and expose simple reporting views for Power BI.

## High-level architecture

```text
CSV files
   ↓
raw        — source-shaped landing tables
   ↓
stg        — cleaned and standardised views
   ↓
int        — reusable joins and transformation logic
   ↓
DQ gate    — key, relationship and business-rule checks
   ↓
scd        — historical customer and item versions
   ↓
dwh        — current source-aligned warehouse tables
   ↓
bus        — reporting-ready business views
   ↓
Power BI
```

## File execution order

Run the project files in this order when setting up the database:

```text
1. createdatabase.sql
2. createschema.sql
3. createstagingviews.sql
4. IntLayer.sql
5. scdschema_sp.sql
6. incrementalloadschema.sql
7. incrementalloadsp.sql
8. busviews.sql
9. Pipeline_cleaned.py
```

The SQL files create the database objects. The Python file runs the recurring ETL process.

---

# End-to-end process

## 1. Create the database and schemas

The project separates each responsibility into its own SQL schema:

```text
raw    Source-shaped landing data
stg    Cleaned source views
int    Reusable transformation logic
scd    Historical dimension versions
dwh    Current warehouse tables
audit  Pipeline tracking and watermarks
bus    Reporting-ready views
```

This separation keeps ingestion, transformation, storage and reporting logic independent.

### Reusable schema pattern

```sql
CREATE SCHEMA raw;
CREATE SCHEMA stg;
CREATE SCHEMA int;
CREATE SCHEMA scd;
CREATE SCHEMA audit;
CREATE SCHEMA dwh;
CREATE SCHEMA bus;
```

---

## 2. Start one audited pipeline run

Python generates one unique `pipeline_run_id` for the complete run and inserts a `RUNNING` record into `audit.pipeline_run`.

That same ID follows the data through raw ingestion, table-load auditing, SCD loads and DWH procedures.

### Pipeline audit template

```python
pipeline_run_id = str(uuid4())
pipeline_start_time = datetime.now()

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
            "pipeline_name": "netsuite_raw_ingestion",
            "start_time": pipeline_start_time,
        },
    )
```

The run is later updated to either `SUCCESS` or `FAILED`.

---

## 3. Load CSV files into the raw layer

A Python dictionary maps each expected CSV filename to its raw SQL table.

```python
RAW_TABLES = {
    "customer": "raw.customer",
    "item": "raw.item",
    "transactions": "raw.transactions",
    "transactionline": "raw.transactionline",
    "sales_budget": "raw.sales_budget",
}
```

Python loops through the folder and only processes recognised files.

The raw layer uses a **full-snapshot replacement pattern**:

1. Read the latest CSV.
2. Delete the previous raw table contents.
3. Append the new snapshot.
4. Validate the loaded row count.
5. Write a table-level audit record.

### Full-snapshot load template

```python
with engine.begin() as connection:
    connection.execute(
        text(f"DELETE FROM {table_name}")
    )

    df.write_database(
        table_name=table_name,
        connection=connection,
        if_table_exists="append",
    )
```

Because the delete, insert and audit write run inside `engine.begin()`, they are committed together or rolled back together.

---

## 4. Add ingestion metadata

Every loaded row receives technical metadata:

```text
pipeline_run_id
ingestion_timestamp
source_file_name
```

### Metadata template

```python
df = df.with_columns(
    pl.lit(pipeline_run_id).alias("pipeline_run_id"),
    pl.lit(datetime.now()).alias("ingestion_timestamp"),
    pl.lit(file_path.name).alias("source_file_name"),
)
```

This makes each row traceable to the pipeline run and source file that produced it.

---

## 5. Reshape the FX file

The FX source stores dates as separate columns. Python converts those columns into rows before loading SQL Server.

### Before

```text
original_currency | target_currency | 31/01/2025 | 28/02/2025
EUR               | USD             | 1.08       | 1.09
```

### After

```text
original_currency | target_currency | rate_date  | avg_rate
EUR               | USD             | 2025-01-31 | 1.08
EUR               | USD             | 2025-02-28 | 1.09
```

### Dynamic unpivot template

```python
date_columns = [
    column
    for column in df.columns
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", column.strip())
]

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
        ),
        pl.col("avg_rate").cast(pl.Float64),
    )
)
```

The date columns are detected dynamically, so the same logic works when new monthly rates are added.

---

## 6. Validate raw row counts

After inserting a DataFrame, the pipeline counts rows loaded with the current `pipeline_run_id`.

If the SQL count differs from the DataFrame count, the load fails.

### Row-count validation template

```python
loaded_row_count = connection.execute(
    text(f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE pipeline_run_id = :pipeline_run_id
    """),
    {"pipeline_run_id": pipeline_run_id},
).scalar_one()

if loaded_row_count != df.height:
    raise ValueError(
        f"Expected {df.height}, loaded {loaded_row_count}."
    )
```

This is a simple reconciliation control between Python and SQL Server.

---

## 7. Clean data through staging views

The staging layer does not physically reload the data. It uses SQL views over the raw tables.

The main staging operations are:

```text
TRIM whitespace
Convert empty strings to NULL
Standardise currency and business-unit codes to uppercase
Expose consistent column names and data types
```

### Staging-cleanup template

```sql
CREATE OR ALTER VIEW stg.example
AS
SELECT
    source_id,
    NULLIF(LTRIM(RTRIM(source_name)), '') AS source_name,
    UPPER(
        NULLIF(LTRIM(RTRIM(currency_code)), '')
    ) AS currency_code,
    pipeline_run_id,
    ingestion_timestamp,
    source_file_name
FROM raw.example;
```

This keeps raw data unchanged while giving later layers a standardised interface.

---

## 8. Build reusable intermediate logic

The `int` layer contains transformations that are useful to multiple downstream objects.

The main examples are:

- Combine transaction-header and transaction-line modification dates.
- Enrich transaction lines with header fields.
- Join items to categories and patterns.
- Add subsidiary and customer attributes.
- Calculate business-unit amounts.
- Prepare budget and deletion records.
- Add the next effective FX date.

### Latest transaction-change template

A transaction must reload when either its header or one of its lines changes.

```sql
WITH LineModified AS
(
    SELECT
        transaction_nsid,
        MAX(transaction_line_last_modified_date)
            AS max_line_modified_date
    FROM stg.transactionline
    GROUP BY transaction_nsid
)
SELECT
    t.transaction_nsid,
    CASE
        WHEN lm.max_line_modified_date >
             t.transaction_last_modified_date
            THEN lm.max_line_modified_date
        ELSE t.transaction_last_modified_date
    END AS transaction_max_modified_date
FROM stg.transactions AS t
LEFT JOIN LineModified AS lm
    ON t.transaction_nsid = lm.transaction_nsid;
```

### Enrichment template

```sql
SELECT
    line.transaction_nsid,
    header.transaction_number,
    customer.customer_name,
    subsidiary.bu_code,
    item.item_name,
    line.foreign_amount * line.bu_rate
        AS amount_bu_currency
FROM transaction_line AS line
JOIN transaction_header AS header
    ON line.transaction_nsid = header.transaction_nsid
LEFT JOIN customer
    ON header.customer_nsid = customer.customer_nsid
LEFT JOIN subsidiary
    ON header.bu_nsid = subsidiary.bu_nsid
LEFT JOIN item
    ON line.item_nsid = item.item_nsid;
```

The intermediate layer prevents the same complex joins from being repeated in every stored procedure or reporting view.

---

## 9. Run data-quality checks

The Python pipeline validates the staging layer before loading the warehouse.

There are three main types of checks.

### Primary-key checks

The pipeline dynamically checks configured keys for:

```text
NULL key values
Duplicate key groups
Composite-key duplicates
```

### Relationship checks

These use a `LEFT JOIN` anti-match pattern to find missing parent records.

```sql
SELECT COUNT(*)
FROM stg.transactionline AS child
LEFT JOIN stg.transactions AS parent
    ON child.transaction_nsid = parent.transaction_nsid
WHERE child.transaction_nsid IS NOT NULL
  AND parent.transaction_nsid IS NULL;
```

### Business-rule checks

Examples include:

```text
Allowed transaction type and status combinations
Required fields
Valid customer tiers
Positive FX and business-unit rates
Three-character currency codes
Month-end budget and FX dates
Delivery dates not earlier than transaction dates
```

### Data-quality gate template

```python
failed_checks = {
    check_name: failed_count
    for check_name, failed_count in all_results.items()
    if failed_count > 0
}

if failed_checks:
    raise RuntimeError(
        f"Data quality gate failed: {failed_checks}"
    )
```

No SCD or DWH load runs when a critical validation fails.

---

## 10. Preserve history with SCD Type 2

Customer and item attributes are stored historically in the `scd` schema.

The SCD process uses a hash of tracked attributes to determine whether a record changed.

### SCD Type 2 flow

```text
Create a hash for the incoming version
        ↓
Compare it with the current stored version
        ↓
If unchanged: do nothing
        ↓
If changed: expire the current row
        ↓
Insert a new current version
```

### Hash template

```sql
HASHBYTES(
    'SHA2_256',
    CONCAT(
        COALESCE(customer_name, ''),
        '|',
        COALESCE(customer_tier, '')
    )
) AS row_hash
```

### Expire changed version

```sql
UPDATE target
SET
    target.effective_to = @load_timestamp,
    target.is_current = 0
FROM scd.customer AS target
JOIN #source_customer AS source
    ON target.customer_nsid = source.customer_nsid
WHERE target.is_current = 1
  AND target.row_hash <> source.row_hash;
```

### Insert new version

```sql
INSERT INTO scd.customer
(
    customer_nsid,
    customer_name,
    row_hash,
    effective_from,
    effective_to,
    is_current
)
SELECT
    source.customer_nsid,
    source.customer_name,
    source.row_hash,
    @load_timestamp,
    '9999-12-31',
    1
FROM #source_customer AS source
WHERE NOT EXISTS
(
    SELECT 1
    FROM scd.customer AS target
    WHERE target.customer_nsid = source.customer_nsid
      AND target.is_current = 1
      AND target.row_hash = source.row_hash
);
```

A filtered unique index ensures there can only be one current version per business key.

---

## 11. Load current DWH tables with hash-based MERGE

Reference-style tables such as customer, item, category, pattern, subsidiary, budget and FX use a reusable `MERGE` pattern.

Before the merge:

1. Standardise the source values.
2. Generate a row hash.
3. Use `ROW_NUMBER()` to retain the latest row for each business key.

### Latest-row template

```sql
WITH LatestSource AS
(
    SELECT
        source_key,
        attribute_1,
        attribute_2,
        HASHBYTES(
            'SHA2_256',
            CONCAT(attribute_1, '|', attribute_2)
        ) AS row_hash,
        ROW_NUMBER() OVER
        (
            PARTITION BY source_key
            ORDER BY
                ingestion_timestamp DESC,
                pipeline_run_id DESC
        ) AS row_number
    FROM stg.source_table
)
```

### MERGE template

```sql
MERGE dwh.target_table AS target
USING
(
    SELECT *
    FROM LatestSource
    WHERE row_number = 1
) AS source
    ON target.source_key = source.source_key

WHEN MATCHED
     AND target.row_hash <> source.row_hash
THEN UPDATE SET
    target.attribute_1 = source.attribute_1,
    target.attribute_2 = source.attribute_2,
    target.row_hash = source.row_hash,
    target.update_timestamp = SYSUTCDATETIME(),
    target.pipeline_run_id = @pipeline_run_id

WHEN NOT MATCHED BY TARGET
THEN INSERT
(
    source_key,
    attribute_1,
    attribute_2,
    row_hash,
    insert_timestamp,
    update_timestamp,
    pipeline_run_id
)
VALUES
(
    source.source_key,
    source.attribute_1,
    source.attribute_2,
    source.row_hash,
    SYSUTCDATETIME(),
    SYSUTCDATETIME(),
    @pipeline_run_id
);
```

The hash prevents unnecessary updates when no tracked attribute changed.

---

## 12. Incrementally load transactions with a watermark

Transactions use a different pattern because headers and lines must stay synchronised.

The pipeline stores the last successfully processed change timestamp in `audit.watermark`.

### Transaction incremental flow

```text
Read and lock the previous watermark
        ↓
Find the newest transaction or deletion timestamp
        ↓
Identify affected transaction IDs
        ↓
Delete their existing child lines
        ↓
Delete their existing headers
        ↓
Reinsert the latest current headers and lines
        ↓
Advance the watermark only after success
```

### Watermark read template

```sql
SELECT
    @last_watermark = COALESCE(
        last_watermark,
        '19000101'
    )
FROM audit.watermark WITH
(
    UPDLOCK,
    HOLDLOCK
)
WHERE process_name = @process_name;
```

`UPDLOCK` and `HOLDLOCK` prevent two concurrent runs from moving the same watermark independently.

### Affected-record template

```sql
INSERT INTO #AffectedTransactions
(
    transaction_nsid
)
SELECT transaction_nsid
FROM int.transaction_modified
WHERE transaction_max_modified_date > @last_watermark
  AND transaction_max_modified_date <= @new_watermark

UNION

SELECT transaction_nsid
FROM int.deleted_transaction
WHERE deleted_date > @last_watermark
  AND deleted_date <= @new_watermark;
```

### Transaction-level delete and reinsert

```sql
DELETE line
FROM dwh.transactionline AS line
JOIN #AffectedTransactions AS affected
    ON line.transaction_nsid =
       affected.transaction_nsid;

DELETE header
FROM dwh.transactions AS header
JOIN #AffectedTransactions AS affected
    ON header.transaction_nsid =
       affected.transaction_nsid;
```

The latest current header and all its current lines are then inserted again.

This pattern is simpler and safer than trying to independently merge every changed line while also handling deleted lines and deleted transactions.

### Safe watermark advancement

```sql
UPDATE audit.watermark
SET
    last_watermark = @new_watermark,
    last_pipeline_run_id = @pipeline_run_id,
    last_run_timestamp = SYSUTCDATETIME(),
    status = 'SUCCESS'
WHERE process_name = @process_name;
```

The watermark moves only after both transaction tables load successfully.

---

## 13. Convert business-unit amounts to USD

Reporting views select the best available monthly FX rate:

1. Use the latest rate on or before the transaction month.
2. When no earlier rate exists, use the earliest future rate.
3. Use a rate of `1` when the business-unit currency is already USD.

### Effective FX lookup template

```sql
OUTER APPLY
(
    SELECT TOP (1)
        rates.avg_rate
    FROM dwh.fx_avg_rate AS rates
    WHERE rates.original_currency = source.bu_currency
      AND rates.target_currency = 'USD'
    ORDER BY
        CASE
            WHEN rates.rate_date <= source.month_end
                THEN 0
            ELSE 1
        END,
        CASE
            WHEN rates.rate_date <= source.month_end
                THEN rates.rate_date
        END DESC,
        CASE
            WHEN rates.rate_date > source.month_end
                THEN rates.rate_date
        END ASC
) AS fx
```

### Currency conversion template

```sql
CASE
    WHEN bu_currency = 'USD'
        THEN amount_bu_currency
    WHEN fx.avg_rate IS NOT NULL
        THEN amount_bu_currency * fx.avg_rate
    ELSE NULL
END AS amount_usd
```

---

## 14. Build reporting-ready business views

The `bus` layer removes technical ETL fields and exposes business-facing data.

The main flow is:

```text
transactionline_enriched
        ↓
transactionline_usd
        ↓
sales_pipeline
        ↓
sales_monthly_summary
        ↓
sales_vs_budget_monthly
```

### Sales classification

```sql
CASE
    WHEN transaction_type = 'Invoice'
        THEN 'ACTUAL'
    WHEN transaction_type = 'Sales Order'
        THEN 'OPEN SALES ORDER'
    WHEN transaction_type = 'Opportunity'
        THEN 'OPEN OPPORTUNITY'
END AS sales_record_type
```

Only the following records enter the sales pipeline:

```text
Fully billed invoices
Open sales orders
Open opportunities
```

### Monthly conditional aggregation

```sql
SUM(
    CASE
        WHEN sales_record_type = 'ACTUAL'
            THEN COALESCE(amount_usd, 0)
        ELSE 0
    END
) AS actual_sales_usd
```

The same pattern produces open sales orders, open opportunities, total open pipeline and estimated landing.

### Budget-to-sales combination

Budget and sales are both aggregated to:

```text
Month
Customer
Business unit
```

A `FULL OUTER JOIN` keeps:

```text
Budget with no sales
Sales with no budget
Matching budget and sales
```

### Variance template

```sql
estimated_sales_landing_usd
-
sales_budget_usd
AS landing_vs_budget_variance_usd
```

### Achievement template

```sql
CASE
    WHEN sales_budget_usd = 0
        THEN NULL
    ELSE estimated_sales_landing_usd
         / sales_budget_usd
END AS landing_budget_achievement_ratio
```

These final views are designed to be imported directly into Power BI.

---

# Main reusable patterns demonstrated

The project is built around a small set of repeatable ETL patterns:

| Pattern | Purpose |
|---|---|
| Full-snapshot raw load | Replace source extracts cleanly |
| Pipeline and table auditing | Trace runs, files and row counts |
| View-based staging | Standardise data without modifying raw records |
| Dynamic FX unpivot | Convert changing date columns into rows |
| `ROW_NUMBER()` latest-record logic | Deduplicate repeated source records |
| Anti-join validation | Find missing parent relationships |
| Data-quality gate | Block bad data before warehouse loading |
| SHA-256 row hashes | Detect actual attribute changes |
| SCD Type 2 | Preserve customer and item history |
| Hash-based `MERGE` | Insert new records and update changed records |
| Watermark incremental load | Process only changed transactions |
| Transaction delete-and-reinsert | Keep headers and lines synchronised |
| Effective-date FX lookup | Select the best monthly conversion rate |
| Conditional aggregation | Build actual, pipeline and landing measures |
| Full outer budget join | Keep unmatched budget and sales records |

---

# Final pipeline sequence

```text
1. Connect to SQL Server
2. Create one pipeline audit run
3. Read recognised CSV files
4. Transform the FX file
5. Add ingestion metadata
6. Replace raw snapshots
7. Reconcile loaded row counts
8. Read cleaned staging views
9. Check keys and duplicates
10. Check parent-child relationships
11. Check business rules
12. Stop the pipeline if validation fails
13. Update SCD Type 2 history
14. MERGE current reference tables
15. Incrementally reload affected transactions
16. Advance the transaction watermark
17. Expose reporting-ready business views
18. Mark the pipeline SUCCESS
19. On any failure, mark it FAILED and retain the error
```

## Result

The final design provides:

- Traceable source ingestion
- Standardised staging data
- Reusable transformation logic
- Blocking data-quality controls
- Historical dimension tracking
- Efficient current-state warehouse loading
- Incremental transaction processing
- USD-normalised business reporting
- Power BI-ready sales and budget views
