# Cloud Reporting Automation

> **Portfolio sample:** This project demonstrates Python-based AWS Lambda reporting automation, input validation, reusable metric calculation, API-style responses, and pytest coverage. It does not contain employer/client code or proprietary data.

## Goal

Build a lightweight event-driven reporting workflow that accepts curated business records, validates them, calculates reusable KPIs, and returns a structured JSON response that could be consumed by an internal API or dashboard.

## Flow

```text
Curated Records
    |
    v
AWS Lambda / Python
    |
    +--> Validation & Normalization
    |
    +--> KPI Aggregation
    |
    v
JSON Reporting Response
    |
    v
Internal API / Dashboard Consumer
```

## Metrics Produced

The Lambda sample calculates:

- Total amount
- Order count
- Average order value
- Revenue by region

## Data-Quality Guardrails

The sample rejects or normalizes invalid input by checking:

- Missing `order_id`
- Non-numeric amounts
- Negative amounts
- Missing region values

## Python Automation

[`cost_reporting_lambda.py`](./cost_reporting_lambda.py) demonstrates:

- AWS Lambda-style event handling
- Reusable transformation functions
- Input validation and normalization
- Structured JSON responses
- Basic exception handling
- Regional metric aggregation

## Automated Testing

[`test_cost_reporting_lambda.py`](./test_cost_reporting_lambda.py) provides pytest examples for:

- Filtering invalid rows
- Validating normalized output
- Verifying total, count, average, and regional calculations

## API Contract Example

[`reporting_api_schema.graphql`](./reporting_api_schema.graphql) is included only as an **illustrative API contract** showing how curated metrics could be exposed to a consuming UI. GraphQL is not the focus of this project.

## Technologies

**Python | AWS Lambda Pattern | Pytest | JSON | API Integration | Data Validation | Reporting Automation**

## Repository Contents

- `cost_reporting_lambda.py` — Lambda-style reporting automation
- `test_cost_reporting_lambda.py` — pytest validation examples
- `reporting_api_schema.graphql` — illustrative reporting API contract

## Production Considerations

In a production implementation, this pattern could be extended with API Gateway, IAM-based access, CloudWatch logging/alerts, Secrets Manager, persistent storage, CI/CD, and downstream dashboard integration.
