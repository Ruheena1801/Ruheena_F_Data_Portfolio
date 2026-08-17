# Power BI Analytics Dashboard

> **Portfolio sample:** This project demonstrates a representative BI/reporting design using Snowflake as the analytics source. It does not contain employer/client data or proprietary dashboard assets.

## Goal

Design an analytics-ready reporting layer and Power BI dashboard concept for monitoring sales and operational KPIs across time, region, and product dimensions.

## Architecture

```text
Source Data
    |
    v
Snowflake Curated Tables
    |
    v
Analytics / KPI View
    |
    v
Power BI Semantic Model
    |
    v
Interactive KPI Dashboard
```

## Dashboard KPIs

The sample reporting layer supports metrics such as:

- Total Revenue
- Order Count
- Average Order Value
- Revenue by Region
- Revenue by Product
- Daily and Monthly Revenue Trends

## Data Model

A simple star-schema approach is used for reporting:

- **FactSales** — order-level sales transactions
- **DimDate** — calendar attributes for daily/monthly analysis
- **DimProduct** — product-level attributes
- **DimRegion** — regional reporting attributes

This structure keeps business metrics reusable and simplifies filtering and aggregation in Power BI.

## Snowflake Reporting Layer

The included [`dashboard_metrics.sql`](./dashboard_metrics.sql) demonstrates how curated Snowflake data can be prepared for BI consumption by:

- Aggregating daily sales metrics
- Calculating order counts and average order value
- Supporting regional and product-level analysis
- Producing a reusable analytics view instead of embedding complex logic directly in reports

## Power BI Design

The dashboard concept uses:

- KPI cards for revenue, orders, and average order value
- Trend charts for time-series analysis
- Region and product slicers
- Drill-down from summary metrics to detailed dimensions
- A centralized Snowflake reporting view to keep transformation logic outside the visualization layer

## Technologies

**Power BI | Snowflake | SQL | Star Schema | Data Modeling | Analytics Reporting**

## Repository Contents

- `README.md` — project architecture and dashboard design
- `dashboard_metrics.sql` — sample Snowflake reporting dataset/view

## Note

A `.pbix` file is intentionally not included. This repository focuses on the data-modeling and reporting-layer design that supports a Power BI implementation while avoiding any employer or client-specific assets.
