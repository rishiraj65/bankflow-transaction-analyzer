-- 1. Total transactions per file
SELECT f.file_id, f.batch_name, COUNT(t.transaction_id) AS total_transactions
FROM files f
LEFT JOIN transactions t ON f.file_id = t.file_id
GROUP BY f.file_id, f.batch_name;

-- 2. Total amount per file
SELECT f.file_id, f.batch_name, SUM(ta.transaction_amount) AS total_amount
FROM files f
JOIN transactions t ON f.file_id = t.file_id
JOIN transaction_amounts ta ON t.transaction_id = ta.transaction_id
GROUP BY f.file_id, f.batch_name;

-- 3. Top merchants (by total transaction volume)
SELECT m.merchant_name, COUNT(tc.transaction_id) as tx_count, SUM(ta.transaction_amount) AS total_volume
FROM merchants m
JOIN transaction_channel tc ON m.merchant_id = tc.merchant_id
JOIN transaction_amounts ta ON tc.transaction_id = ta.transaction_id
GROUP BY m.merchant_id, m.merchant_name
ORDER BY total_volume DESC
LIMIT 5;

-- 4. Transaction distribution by type
SELECT transaction_type, COUNT(transaction_id) as tx_count, SUM(ta.transaction_amount) as total_volume
FROM transactions t
JOIN transaction_amounts ta ON t.transaction_id = ta.transaction_id
GROUP BY transaction_type
ORDER BY tx_count DESC;
