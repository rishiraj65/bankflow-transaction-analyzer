-- Dropping existing tables and recreating with VARCHAR(255) for all numeric/boolean/etc. fields
DROP TABLE IF EXISTS audit_log CASCADE;
DROP TABLE IF EXISTS compliance CASCADE;
DROP TABLE IF EXISTS balances CASCADE;
DROP TABLE IF EXISTS transaction_security CASCADE;
DROP TABLE IF EXISTS transaction_channel CASCADE;
DROP TABLE IF EXISTS transaction_amounts CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS beneficiary CASCADE;
DROP TABLE IF EXISTS merchants CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS files CASCADE;

CREATE TABLE files (
    file_id VARCHAR(100) PRIMARY KEY,
    batch_name VARCHAR(255) NOT NULL,
    total_records VARCHAR(255) NOT NULL,
    generated_at VARCHAR(255) NOT NULL,
    from_date VARCHAR(255),
    to_date VARCHAR(255)
);

CREATE TABLE customers (
    customer_id VARCHAR(100) PRIMARY KEY,
    account_holder_name VARCHAR(255) NOT NULL,
    mobile_number VARCHAR(100),
    email_id VARCHAR(255),
    pan_number VARCHAR(100),
    customer_segment VARCHAR(100)
);

CREATE TABLE accounts (
    account_number VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100) REFERENCES customers(customer_id) ON DELETE CASCADE,
    account_type VARCHAR(100),
    branch_code VARCHAR(100),
    iban VARCHAR(100),
    swift_code VARCHAR(100),
    account_status VARCHAR(100),
    account_open_date VARCHAR(100)
);

CREATE TABLE merchants (
    merchant_id VARCHAR(100) PRIMARY KEY,
    merchant_name VARCHAR(255),
    merchant_category_code VARCHAR(100),
    location VARCHAR(255)
);

CREATE TABLE beneficiary (
    beneficiary_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(255),
    account_number VARCHAR(100),
    bank_name VARCHAR(255),
    city VARCHAR(255),
    type VARCHAR(100)
);

CREATE TABLE transactions (
    transaction_id VARCHAR(100) PRIMARY KEY,
    account_number VARCHAR(100) REFERENCES accounts(account_number),
    file_id VARCHAR(100) REFERENCES files(file_id),
    beneficiary_id VARCHAR(100) REFERENCES beneficiary(beneficiary_id),
    reference_number VARCHAR(255),
    transaction_datetime VARCHAR(255) NOT NULL,
    transaction_type VARCHAR(100) NOT NULL,
    transaction_category VARCHAR(255),
    transaction_mode VARCHAR(100),
    transaction_purpose VARCHAR(255),
    status VARCHAR(100)
);

CREATE TABLE transaction_amounts (
    transaction_id VARCHAR(100) PRIMARY KEY REFERENCES transactions(transaction_id),
    transaction_amount VARCHAR(255) NOT NULL,
    currency_code VARCHAR(100) NOT NULL,
    converted_amount VARCHAR(255),
    transaction_fees VARCHAR(255),
    tax_amount VARCHAR(255),
    net_amount VARCHAR(255) NOT NULL
);

CREATE TABLE transaction_channel (
    transaction_id VARCHAR(100) PRIMARY KEY REFERENCES transactions(transaction_id),
    merchant_id VARCHAR(100) REFERENCES merchants(merchant_id),
    channel_type VARCHAR(100),
    device_id VARCHAR(100),
    network_type VARCHAR(100),
    latitude VARCHAR(255),
    longitude VARCHAR(255)
);

CREATE TABLE transaction_security (
    transaction_id VARCHAR(100) PRIMARY KEY REFERENCES transactions(transaction_id),
    authentication_method VARCHAR(255),
    card_type VARCHAR(255),
    risk_score VARCHAR(255),
    fraud_flag VARCHAR(255)
);

CREATE TABLE balances (
    transaction_id VARCHAR(100) PRIMARY KEY REFERENCES transactions(transaction_id),
    opening_balance VARCHAR(255),
    closing_balance VARCHAR(255),
    available_balance VARCHAR(255)
);

CREATE TABLE compliance (
    transaction_id VARCHAR(100) PRIMARY KEY REFERENCES transactions(transaction_id),
    kyc_status VARCHAR(255),
    risk_category VARCHAR(255),
    aml_flag VARCHAR(255)
);

CREATE TABLE audit_log (
    transaction_id VARCHAR(100) PRIMARY KEY REFERENCES transactions(transaction_id),
    created_by VARCHAR(255),
    processing_time_ms VARCHAR(255),
    core_banking_module VARCHAR(255)
);
