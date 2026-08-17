-- Power BI Analytics Dashboard - Snowflake reporting layer (portfolio sample)
-- Demonstrates reusable KPI logic for BI consumption.

CREATE OR REPLACE VIEW analytics.vw_daily_sales_metrics AS
SELECT
    event_date,
    region,
    product_category,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_order_value
FROM analytics.sales_curated
WHERE order_id IS NOT NULL
  AND event_date IS NOT NULL
  AND amount IS NOT NULL
GROUP BY
    event_date,
    region,
    product_category;

-- Example summary query for a Power BI KPI card
SELECT
    SUM(total_revenue) AS total_revenue,
    SUM(order_count) AS total_orders,
    CASE
        WHEN SUM(order_count) = 0 THEN NULL
        ELSE SUM(total_revenue) / SUM(order_count)
    END AS avg_order_value
FROM analytics.vw_daily_sales_metrics;

-- Example trend query
SELECT
    event_date,
    SUM(total_revenue) AS daily_revenue,
    SUM(order_count) AS daily_orders
FROM analytics.vw_daily_sales_metrics
GROUP BY event_date
ORDER BY event_date;
