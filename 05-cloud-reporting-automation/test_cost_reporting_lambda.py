from cost_reporting_lambda import normalize_records, transform


def test_normalize_records_filters_invalid_rows():
    records = [
        {"order_id": "1001", "amount": 10, "region": "west"},
        {"order_id": None, "amount": 20, "region": "east"},
        {"order_id": "1003", "amount": -5, "region": "east"},
        {"order_id": "1004", "amount": "bad", "region": "south"},
    ]

    cleaned = normalize_records(records)

    assert cleaned == [
        {"order_id": "1001", "amount": 10.0, "region": "WEST"}
    ]


def test_transform_calculates_reporting_metrics():
    records = [
        {"order_id": "1001", "amount": 125.50, "region": "west"},
        {"order_id": "1002", "amount": 74.50, "region": "west"},
        {"order_id": "1003", "amount": 50.00, "region": "east"},
    ]

    result = transform(records)

    assert result["order_count"] == 3
    assert result["total_amount"] == 250.0
    assert result["avg_order_value"] == 83.33
    assert result["revenue_by_region"] == {
        "WEST": 200.0,
        "EAST": 50.0,
    }
