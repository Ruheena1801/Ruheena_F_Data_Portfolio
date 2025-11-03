# Example AWS Lambda-style function (runnable as plain Python)
import json
from datetime import date

def transform(records):
    total = sum(r.get("amount", 0.0) for r in records)
    count = len(records)
    avg = round(total / count, 2) if count else 0.0
    return {"total_amount": total, "count": count, "avg_amount": avg, "date": str(date.today())}

def handler(event, context=None):
    records = event.get("records", [])
    result = transform(records)
    return {"statusCode": 200, "body": json.dumps(result)}

if __name__ == "__main__":
    sample = {"records":[{"amount": 10.5},{"amount": 4.5},{"amount": 5.0}]}
    print(handler(sample))
