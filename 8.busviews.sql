/*==============================================================
  CREATE BUSINESS SCHEMA
==============================================================*/

IF NOT EXISTS
(
    SELECT 1
    FROM sys.schemas
    WHERE name = 'bus'
)
BEGIN
    EXEC('CREATE SCHEMA bus');
END;
GO


/*==============================================================
  1. ENRICHED TRANSACTION-LINE DATA

  Grain:
  One row per transaction line.

  Technical IDs and ETL metadata are excluded.
==============================================================*/

CREATE OR ALTER VIEW bus.transactionline_enriched
AS
SELECT
    transactions.transaction_number,
    transactions.transaction_type,
    transactions.transaction_status,
    transactions.transaction_date,

    EOMONTH(transactions.transaction_date)
        AS transaction_month_end,

    transactions.expected_delivery_date,

    customer.customer_name,
    customer.customer_tier,

    subsidiary.bu_code,
    subsidiary.bu_country_code,
    subsidiary.bu_currency,
    subsidiary.bu_legal_name,
    subsidiary.bu_commercial_group,

    item.item_name,
    item.item_code,
    item.item_type,
    item.project_code,

    item_category.item_category,
    item_category.item_sub_category,

    item_pattern.item_pattern,

    transactionline.quantity,
    transactionline.foreign_amount,
    transactionline.foreign_currency,
    transactionline.bu_rate,

    CAST
    (
        transactionline.foreign_amount
        * transactionline.bu_rate
        AS DECIMAL(38, 10)
    ) AS amount_bu_currency

FROM dwh.transactions AS transactions

INNER JOIN dwh.transactionline AS transactionline
    ON transactions.transaction_nsid =
       transactionline.transaction_nsid

LEFT JOIN dwh.customer AS customer
    ON transactions.customer_nsid =
       customer.customer_nsid

LEFT JOIN dwh.subsidiary AS subsidiary
    ON transactions.bu_nsid =
       subsidiary.bu_nsid

LEFT JOIN dwh.item AS item
    ON transactionline.item_nsid =
       item.item_nsid

LEFT JOIN dwh.item_category AS item_category
    ON item.item_category_nsid =
       item_category.item_category_nsid

LEFT JOIN dwh.item_pattern AS item_pattern
    ON item.item_pattern_nsid =
       item_pattern.item_pattern_nsid;
GO


/*==============================================================
  2. TRANSACTION-LINE DATA CONVERTED TO USD

  FX logic:
  - Use the most recent rate on or before the transaction month.
  - If no earlier rate exists, use the earliest available future rate.
  - USD to USD uses a rate of 1.
==============================================================*/

CREATE OR ALTER VIEW bus.transactionline_usd
AS
SELECT
    transactionline.transaction_number,
    transactionline.transaction_type,
    transactionline.transaction_status,
    transactionline.transaction_date,
    transactionline.transaction_month_end,
    transactionline.expected_delivery_date,

    transactionline.customer_name,
    transactionline.customer_tier,

    transactionline.bu_code,
    transactionline.bu_country_code,
    transactionline.bu_currency,
    transactionline.bu_legal_name,
    transactionline.bu_commercial_group,

    transactionline.item_name,
    transactionline.item_code,
    transactionline.item_type,
    transactionline.project_code,
    transactionline.item_category,
    transactionline.item_sub_category,
    transactionline.item_pattern,

    transactionline.quantity,
    transactionline.foreign_amount,
    transactionline.foreign_currency,
    transactionline.bu_rate,
    transactionline.amount_bu_currency,

    CASE
        WHEN transactionline.bu_currency = 'USD'
            THEN CAST(1 AS DECIMAL(38, 18))
        ELSE fx.avg_rate
    END AS usd_fx_rate,

    CASE
        WHEN transactionline.bu_currency = 'USD'
        THEN
            transactionline.amount_bu_currency

        WHEN fx.avg_rate IS NOT NULL
        THEN
            CAST
            (
                transactionline.amount_bu_currency
                * fx.avg_rate
                AS DECIMAL(38, 10)
            )

        ELSE NULL
    END AS amount_usd

FROM bus.transactionline_enriched AS transactionline

OUTER APPLY
(
    SELECT TOP (1)
        rates.avg_rate
    FROM dwh.fx_avg_rate AS rates
    WHERE rates.original_currency =
          transactionline.bu_currency
      AND rates.target_currency = 'USD'
    ORDER BY
        CASE
            WHEN rates.rate_date <=
                 transactionline.transaction_month_end
                THEN 0
            ELSE 1
        END,

        CASE
            WHEN rates.rate_date <=
                 transactionline.transaction_month_end
                THEN rates.rate_date
        END DESC,

        CASE
            WHEN rates.rate_date >
                 transactionline.transaction_month_end
                THEN rates.rate_date
        END ASC
) AS fx;
GO


/*==============================================================
  3. SALES PIPELINE

  Includes:
  - Fully billed invoices
  - Open sales orders
  - Open opportunities
==============================================================*/

CREATE OR ALTER VIEW bus.sales_pipeline
AS
SELECT
    transaction_number,
    transaction_type,
    transaction_status,

    CASE
        WHEN transaction_type = 'Invoice'
            THEN 'ACTUAL'

        WHEN transaction_type = 'Sales Order'
            THEN 'OPEN SALES ORDER'

        WHEN transaction_type = 'Opportunity'
            THEN 'OPEN OPPORTUNITY'
    END AS sales_record_type,

    transaction_date,
    expected_delivery_date,

    CASE
        WHEN transaction_type = 'Invoice'
            THEN transaction_month_end

        WHEN expected_delivery_date IS NOT NULL
            THEN EOMONTH(expected_delivery_date)

        ELSE transaction_month_end
    END AS sales_month_end,

    customer_name,
    customer_tier,

    bu_code,
    bu_country_code,
    bu_currency,
    bu_legal_name,
    bu_commercial_group,

    item_name,
    item_code,
    item_type,
    project_code,
    item_category,
    item_sub_category,
    item_pattern,

    quantity,
    foreign_amount,
    foreign_currency,
    amount_bu_currency,
    amount_usd

FROM bus.transactionline_usd

WHERE
(
    transaction_type = 'Invoice'
    AND transaction_status = 'Fully Billed'
)
OR
(
    transaction_type IN
    (
        'Sales Order',
        'Opportunity'
    )
    AND transaction_status IN
    (
        'Under Discussion',
        'Ongoing'
    )
);
GO


/*==============================================================
  4. SALES BUDGET CONVERTED TO USD

  Grain:
  Budget version, month, customer name and business unit.

  Budget remains separate because it has no item-level grain.
==============================================================*/

CREATE OR ALTER VIEW bus.sales_budget
AS
SELECT
    budget.budget_year,
    budget.budget_version,
    budget.budget_date,

    CASE
        WHEN MONTH(budget.budget_date) = 1
            THEN YEAR(budget.budget_date) - 1
        ELSE YEAR(budget.budget_date)
    END AS fiscal_year,

    CASE
        WHEN MONTH(budget.budget_date) = 1
            THEN 12
        ELSE MONTH(budget.budget_date) - 1
    END AS fiscal_month_number,

    budget.customer_name,

    budget.bu_code,
    budget.bu_currency,

    budget.sales_amount_bu_currency,

    CASE
        WHEN budget.bu_currency = 'USD'
        THEN
            budget.sales_amount_bu_currency

        WHEN fx.avg_rate IS NOT NULL
        THEN
            CAST
            (
                budget.sales_amount_bu_currency
                * fx.avg_rate
                AS DECIMAL(38, 10)
            )

        ELSE NULL
    END AS sales_budget_usd

FROM dwh.sales_budget AS budget

OUTER APPLY
(
    SELECT TOP (1)
        rates.avg_rate
    FROM dwh.fx_avg_rate AS rates
    WHERE rates.original_currency =
          budget.bu_currency
      AND rates.target_currency = 'USD'
    ORDER BY
        CASE
            WHEN rates.rate_date <= budget.budget_date
                THEN 0
            ELSE 1
        END,

        CASE
            WHEN rates.rate_date <= budget.budget_date
                THEN rates.rate_date
        END DESC,

        CASE
            WHEN rates.rate_date > budget.budget_date
                THEN rates.rate_date
        END ASC
) AS fx;
GO


/*==============================================================
  5. MONTHLY SALES SUMMARY

  Grain:
  Month, customer and business unit.
==============================================================*/

CREATE OR ALTER VIEW bus.sales_monthly_summary
AS
SELECT
    sales_month_end,

    CASE
        WHEN MONTH(sales_month_end) = 1
            THEN YEAR(sales_month_end) - 1
        ELSE YEAR(sales_month_end)
    END AS fiscal_year,

    CASE
        WHEN MONTH(sales_month_end) = 1
            THEN 12
        ELSE MONTH(sales_month_end) - 1
    END AS fiscal_month_number,

    customer_name,
    customer_tier,

    bu_code,
    bu_country_code,
    bu_currency,
    bu_legal_name,
    bu_commercial_group,

    SUM
    (
        CASE
            WHEN sales_record_type = 'ACTUAL'
                THEN COALESCE(amount_usd, 0)
            ELSE 0
        END
    ) AS actual_sales_usd,

    SUM
    (
        CASE
            WHEN sales_record_type = 'OPEN SALES ORDER'
                THEN COALESCE(amount_usd, 0)
            ELSE 0
        END
    ) AS open_sales_order_usd,

    SUM
    (
        CASE
            WHEN sales_record_type = 'OPEN OPPORTUNITY'
                THEN COALESCE(amount_usd, 0)
            ELSE 0
        END
    ) AS open_opportunity_usd,

    SUM
    (
        CASE
            WHEN sales_record_type IN
            (
                'OPEN SALES ORDER',
                'OPEN OPPORTUNITY'
            )
                THEN COALESCE(amount_usd, 0)
            ELSE 0
        END
    ) AS total_open_pipeline_usd,

    SUM
    (
        COALESCE(amount_usd, 0)
    ) AS estimated_sales_landing_usd

FROM bus.sales_pipeline

GROUP BY
    sales_month_end,

    customer_name,
    customer_tier,

    bu_code,
    bu_country_code,
    bu_currency,
    bu_legal_name,
    bu_commercial_group;
GO


/*==============================================================
  6. MONTHLY SALES VERSUS BUDGET

  Grain:
  Budget version, month, customer and business unit.
==============================================================*/

CREATE OR ALTER VIEW bus.sales_vs_budget_monthly
AS
WITH MonthlyBudget AS
(
    SELECT
        budget_version,
        budget_date,
        fiscal_year,
        fiscal_month_number,
        customer_name,
        bu_code,
        bu_currency,

        SUM
        (
            sales_amount_bu_currency
        ) AS sales_budget_bu_currency,

        SUM
        (
            sales_budget_usd
        ) AS sales_budget_usd

    FROM bus.sales_budget

    GROUP BY
        budget_version,
        budget_date,
        fiscal_year,
        fiscal_month_number,
        customer_name,
        bu_code,
        bu_currency
)
SELECT
    budget.budget_version,

    COALESCE
    (
        budget.budget_date,
        sales.sales_month_end
    ) AS period_end,

    COALESCE
    (
        budget.fiscal_year,
        sales.fiscal_year
    ) AS fiscal_year,

    COALESCE
    (
        budget.fiscal_month_number,
        sales.fiscal_month_number
    ) AS fiscal_month_number,

    COALESCE
    (
        budget.customer_name,
        sales.customer_name
    ) AS customer_name,

    sales.customer_tier,

    COALESCE
    (
        budget.bu_code,
        sales.bu_code
    ) AS bu_code,

    COALESCE
    (
        budget.bu_currency,
        sales.bu_currency
    ) AS bu_currency,

    sales.bu_country_code,
    sales.bu_legal_name,
    sales.bu_commercial_group,

    COALESCE
    (
        sales.actual_sales_usd,
        0
    ) AS actual_sales_usd,

    COALESCE
    (
        sales.open_sales_order_usd,
        0
    ) AS open_sales_order_usd,

    COALESCE
    (
        sales.open_opportunity_usd,
        0
    ) AS open_opportunity_usd,

    COALESCE
    (
        sales.total_open_pipeline_usd,
        0
    ) AS total_open_pipeline_usd,

    COALESCE
    (
        sales.estimated_sales_landing_usd,
        0
    ) AS estimated_sales_landing_usd,

    COALESCE
    (
        budget.sales_budget_usd,
        0
    ) AS sales_budget_usd,

    COALESCE
    (
        sales.actual_sales_usd,
        0
    )
    -
    COALESCE
    (
        budget.sales_budget_usd,
        0
    ) AS actual_vs_budget_variance_usd,

    COALESCE
    (
        sales.estimated_sales_landing_usd,
        0
    )
    -
    COALESCE
    (
        budget.sales_budget_usd,
        0
    ) AS landing_vs_budget_variance_usd,

    CASE
        WHEN COALESCE(
            budget.sales_budget_usd,
            0
        ) = 0
        THEN NULL

        ELSE
            COALESCE(
                sales.actual_sales_usd,
                0
            )
            /
            budget.sales_budget_usd
    END AS actual_budget_achievement_ratio,

    CASE
        WHEN COALESCE(
            budget.sales_budget_usd,
            0
        ) = 0
        THEN NULL

        ELSE
            COALESCE(
                sales.estimated_sales_landing_usd,
                0
            )
            /
            budget.sales_budget_usd
    END AS landing_budget_achievement_ratio

FROM MonthlyBudget AS budget

FULL OUTER JOIN bus.sales_monthly_summary AS sales
    ON budget.budget_date =
       sales.sales_month_end

   AND UPPER(LTRIM(RTRIM(budget.customer_name))) =
       UPPER(LTRIM(RTRIM(sales.customer_name)))

   AND UPPER(LTRIM(RTRIM(budget.bu_code))) =
       UPPER(LTRIM(RTRIM(sales.bu_code)));
GO


/*==============================================================
  7. ACTUAL SALES ONLY
==============================================================*/

CREATE OR ALTER VIEW bus.sales_actuals
AS
SELECT
    transaction_number,
    transaction_type,
    transaction_status,
    transaction_date,
    sales_month_end,

    customer_name,
    customer_tier,

    bu_code,
    bu_country_code,
    bu_currency,
    bu_legal_name,
    bu_commercial_group,

    item_name,
    item_code,
    item_type,
    project_code,
    item_category,
    item_sub_category,
    item_pattern,

    quantity,
    amount_bu_currency,
    amount_usd

FROM bus.sales_pipeline

WHERE sales_record_type = 'ACTUAL';
GO


/*==============================================================
  8. OPEN SALES PIPELINE ONLY
==============================================================*/

CREATE OR ALTER VIEW bus.open_sales_pipeline
AS
SELECT
    transaction_number,
    transaction_type,
    transaction_status,
    sales_record_type,

    transaction_date,
    expected_delivery_date,
    sales_month_end,

    customer_name,
    customer_tier,

    bu_code,
    bu_country_code,
    bu_currency,
    bu_legal_name,
    bu_commercial_group,

    item_name,
    item_code,
    item_type,
    project_code,
    item_category,
    item_sub_category,
    item_pattern,

    quantity,
    amount_bu_currency,
    amount_usd

FROM bus.sales_pipeline

WHERE sales_record_type IN
(
    'OPEN SALES ORDER',
    'OPEN OPPORTUNITY'
);
GO