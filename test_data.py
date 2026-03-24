import json
from datetime import datetime

data = {
    "batch_metadata": {
        "batch_id": "BATCH_001",
        "batch_name": "Daily_Txns",
        "generated_time": datetime.utcnow().isoformat(),
        "from_date": "2023-10-01T00:00:00.000000",
        "to_date": "2023-10-01T23:59:59.000000"
    },
    "transactions": [
        {
            "transaction_identification": {
                "transaction_id": "TXN_0001",
                "reference_number": "REF123"
            },
            "account_information": {
                "account_number": "ACC_1001",
                "customer_id": "CUST_501",
                "account_holder_name": "Alice Smith",
                "mobile_number": "1234567890",
                "account_type": "SAVINGS"
            },
            "transaction_details": {
                "transaction_datetime": datetime.utcnow().isoformat(),
                "transaction_type": "DEBIT",
                "transaction_mode": "ONLINE",
                "transaction_category": "PURCHASE"
            },
            "amount_and_currency": {
                "transaction_amount": 150.00,
                "currency_code": "USD",
                "net_amount": 150.00
            },
            "channel_information": {
                "merchant_id": "MERCH_88",
                "merchant_name": "Amazon",
                "channel_type": "E-COMMERCE"
            }
        },
        {
            "transaction_identification": {
                "transaction_id": "TXN_0002",
                "reference_number": "REF124"
            },
            "account_information": {
                "account_number": "ACC_1002",
                "customer_id": "CUST_502",
                "account_holder_name": "Bob Jones",
                "mobile_number": "0987654321",
                "account_type": "CHECKING"
            },
            "transaction_details": {
                "transaction_datetime": datetime.utcnow().isoformat(),
                "transaction_type": "CREDIT",
                "transaction_mode": "ATM",
                "transaction_category": "DEPOSIT"
            },
            "amount_and_currency": {
                "transaction_amount": 2000.00,
                "currency_code": "USD",
                "net_amount": 2000.00
            }
        }
    ]
}

with open("mock_transactions.json", "w") as f:
    json.dump(data, f, indent=4)
print("mock_transactions.json created successfully")
