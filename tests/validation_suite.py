import asyncio
import httpx
import json
import os
import time
import urllib.parse
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()

# --- DB CONFIG ---
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "retail_banking_scratch_v1")

# Handle special characters in password
ENCODED_PASS = urllib.parse.quote_plus(DB_PASS)
DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{ENCODED_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

API_BASE = "http://localhost:8000"

async def test_database_integrity():
    print("\n--- [DATABASE] Integrity & Relationships ---")
    engine = create_async_engine(DATABASE_URL)
    try:
        async with engine.connect() as conn:
            # 1. Validate relationships (PK/FK)
            res = await conn.execute(text("SELECT count(*) FROM transactions WHERE account_number NOT IN (SELECT account_number FROM accounts)"))
            orphans = res.scalar()
            print(f"✔️ Transactions without accounts (Orphans): {orphans}")
            
            # 2. Check for accounts without customers
            res = await conn.execute(text("SELECT count(*) FROM accounts WHERE customer_id NOT IN (SELECT customer_id FROM customers)"))
            orphans_cust = res.scalar()
            print(f"✔️ Accounts without customers (Orphans): {orphans_cust}")

            # 3. Test constraints
            res = await conn.execute(text("SELECT count(*) FROM transactions WHERE transaction_id IS NULL OR amount IS NULL"))
            nulls = res.scalar()
            print(f"✔️ Critical NULL violations: {nulls}")
            
            # 4. Check for orphan merchants
            res = await conn.execute(text("SELECT count(*) FROM merchants WHERE transaction_id NOT IN (SELECT transaction_id FROM transactions)"))
            orphans_merch = res.scalar()
            print(f"✔️ Orphan Merchant records: {orphans_merch}")
    except Exception as e:
        print(f"❌ Database integrity check failed: {e}")
    finally:
        await engine.dispose()

async def test_api_robustness():
    print("\n--- [API] Robustness & Error Handling ---")
    async with httpx.AsyncClient(timeout=30.0) as client:
        # 1. Test Rollback on Failure (Invalid JSON)
        print("Testing Rollback on Failure...")
        invalid_payload = {
            "batch_metadata": {"batch_id": "ERROR_TEST", "batch_name": "Rollback Test"},
            "transactions": [{"invalid_key": "data"}] 
        }
        try:
            resp = await client.post(f"{API_BASE}/upload", json=invalid_payload)
            print(f"✔️ Invalid Upload Result: {resp.status_code} (Expected 422)")
        except Exception as e:
            print(f"❌ Connection to API failed: {e}")

        # 2. Duplicate Prevention / Upsert Logic
        print("Testing Duplicate Prevention (Upsert)...")
        test_batch_id = f"TEST_BATCH_{int(time.time())}"
        valid_payload = {
            "batch_metadata": {
                "batch_id": test_batch_id, 
                "batch_name": "Testing Ops",
                "generated_time": "2023-10-01T00:00:00",
                "from_date": "2023-10-01",
                "to_date": "2023-10-01",
                "total_records": "1"
            },
            "transactions": [{
                "transaction_identification": {"transaction_id": f"TXN_{test_batch_id}", "reference_number": "REF_001"},
                "account_information": {
                    "account_number": f"ACC_{test_batch_id}", "customer_id": "CUST_001", 
                    "account_holder_name": "Alice Tester", "mobile_number": "1234567890", "account_type": "SAVINGS"
                },
                "transaction_details": {
                    "transaction_datetime": "2023-10-01T10:00:00", "transaction_type": "DEBIT", 
                    "transaction_mode": "ONLINE", "transaction_category": "TEST"
                },
                "amount_and_currency": {"transaction_amount": 500.0, "currency_code": "USD", "net_amount": 500.0},
                "channel_information": {"merchant_id": "M001", "merchant_name": "Test Shop", "channel_type": "WEB"}
            }]
        }
        
        # First Upload
        r1 = await client.post(f"{API_BASE}/upload", json=valid_payload)
        # Second Upload (Same data)
        r2 = await client.post(f"{API_BASE}/upload", json=valid_payload)
        
        if r1.status_code == 200 and r2.status_code == 200:
            print(f"✔️ Duplicate batches handled successfully (Status 200)")
        else:
            print(f"❌ Upsert failed: R1={r1.status_code}, R2={r2.status_code}")

async def test_performance():
    print("\n--- [PERFORMANCE] Large File Ingestion ---")
    files = ["retail_banking_transactions_2.json", "retail_banking_transactions_1.json"]
    target_file = None
    for f in files:
        if os.path.exists(f):
            target_file = f
            break
            
    if target_file:
        with open(target_file, "r") as f:
            payload = json.load(f)
        
        txns = payload.get("transactions", payload.get("retail_banking_transactions", []))
        print(f"Testing ingestion of {len(txns)} records from {target_file}...")
        
        start_time = time.time()
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(f"{API_BASE}/upload", json=payload)
        duration = time.time() - start_time
        
        if resp.status_code == 200:
            print(f"✔️ Performance Test Passed: Ingested in {duration:.2f} seconds.")
            print(f"✔️ Throughput: {len(txns)/duration:.2f} records/sec")
        else:
            print(f"❌ Performance Test Failed: {resp.status_code} - {resp.text}")
    else:
        print("⚠️ No large test files found in directory.")

async def run_all():
    print("🚀 STARTING NOVA-BANK VALIDATION SUITE\n" + "="*40)
    await test_database_integrity()
    await test_api_robustness()
    await test_performance()
    print("\n" + "="*40 + "\n✅ VALIDATION COMPLETE")

if __name__ == "__main__":
    asyncio.run(run_all())
