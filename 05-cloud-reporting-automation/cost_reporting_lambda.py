# Cloud Reporting Automation (portfolio sample)
# AWS Lambda-style Python function that validates reporting records,
# calculates reusable business metrics, and returns a JSON API response.

import json
from datetime import date
from typing import Any, Dict, Iterable, List


def normalize_records(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Validate and normalize incoming reporting records."""
    cleaned = []

    for record in records:
        if not record.get("order_id"):
            continue

        try:
            amount = float(record.get("amount", 0.0))
        except (TypeError, ValueError):
            continue

        if amount < 0:
            continue

        cleaned.append(
            {
                "order_id": str(record["order_id"]),
                "amount": amount,
                "region": str(record.get("region", "UNKNOWN")).upper(),
            }
        )

    return cleaned


def transform(records: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Build a compact reporting payload from validated records."""
    cleaned = normalize_records(records)

    total_amount = round(sum(record["amount"] for record in cleaned), 2)
    order_count = len(cleaned)
    avg_order_value = round(total_amount / order_count, 2) if order_count else 0.0

    revenue_by_region: Dict[str, float] = {}
    for record in cleaned:
        region = record["region"]
        revenue_by_region[region] = round(
            revenue_by_region.get(region, 0.0) + record["amount"],
            2,
        )

    return {
        "report_date": str(date.today()),
        "order_count": order_count,
        "total_amount": total_amount,
        "avg_order_value": avg_order_value,
        "revenue_by_region": revenue_by_region,
    }


def handler(event: Dict[str, Any], context=None) -> Dict[str, Any]:
    """Lambda entry point for an API/event-driven reporting workflow."""
    try:
        records = event.get("records", [])
        result = transform(records)

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(result),
        }
    except Exception as exc:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "report_generation_failed", "detail": str(exc)}),
        }


if __name__ == "__main__":
    sample_event = {
        "records": [
            {"order_id": "1001", "amount": 125.50, "region": "west"},
            {"order_id": "1002", "amount": 74.50, "region": "west"},
            {"order_id": "1003", "amount": 50.00, "region": "east"},
        ]
    }

    print(handler(sample_event))
