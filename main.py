import os
import json
import logging
import urllib.parse
from datetime import date, datetime
from typing import List, Optional, Dict, Any

from dotenv import load_dotenv
from fastapi import FastAPI, Depends, HTTPException, status, UploadFile, File as FastAPIFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import relationship, DeclarativeBase
from sqlalchemy import Column, String, Integer, Float, ForeignKey, DateTime, select, func, desc, cast, Numeric, delete, text
from sqlalchemy.dialects.postgresql import insert
from pydantic import BaseModel, Field, model_validator

load_dotenv()

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- DATABASE CONFIG ---
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "Rishi@2005")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "retail_banking_scratch_v1")

# Handle special characters in password
ENCODED_PASS = urllib.parse.quote_plus(DB_PASS)
DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{ENCODED_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

class Base(DeclarativeBase):
    pass

# --- MODELS ---
class FileInfo(Base):
    __tablename__ = "files"
    file_id = Column(String(100), primary_key=True)
    batch_name = Column(String(255), nullable=False)
    total_records = Column(String(255), nullable=False)
    generated_at = Column(String(255), nullable=False)
    from_date = Column(String(255))
    to_date = Column(String(255))
    transactions = relationship("Transaction", back_populates="file")

class Customer(Base):
    __tablename__ = "customers"
    customer_id = Column(String(100), primary_key=True)
    account_holder_name = Column(String(255), nullable=False)
    mobile_number = Column(String(100))
    email_id = Column(String(255))
    pan_number = Column(String(100))
    customer_segment = Column(String(100))
    accounts = relationship("Account", back_populates="customer")

class Account(Base):
    __tablename__ = "accounts"
    account_number = Column(String(100), primary_key=True)
    customer_id = Column(String(100), ForeignKey("customers.customer_id", ondelete="CASCADE"), nullable=False)
    account_type = Column(String(100))
    branch_code = Column(String(100))
    iban = Column(String(100))
    swift_code = Column(String(100))
    account_status = Column(String(100))
    account_open_date = Column(String(100))
    customer = relationship("Customer", back_populates="accounts")
    transactions = relationship("Transaction", back_populates="account")

class Merchant(Base):
    __tablename__ = "merchants"
    merchant_id = Column(String(100), primary_key=True)
    merchant_name = Column(String(255))
    merchant_category_code = Column(String(100))
    location = Column(String(255))
    transaction_channels = relationship("TransactionChannel", back_populates="merchant")

class Beneficiary(Base):
    __tablename__ = "beneficiary"
    beneficiary_id = Column(String(100), primary_key=True)
    name = Column(String(255))
    account_number = Column(String(100))
    bank_name = Column(String(255))
    city = Column(String(255))
    type = Column(String(100))
    transactions = relationship("Transaction", back_populates="beneficiary")

class Transaction(Base):
    __tablename__ = "transactions"
    transaction_id = Column(String(100), primary_key=True)
    account_number = Column(String(100), ForeignKey("accounts.account_number"), nullable=False)
    file_id = Column(String(100), ForeignKey("files.file_id"), nullable=False)
    beneficiary_id = Column(String(100), ForeignKey("beneficiary.beneficiary_id"))
    reference_number = Column(String(255))
    transaction_datetime = Column(String(255), nullable=False)
    transaction_type = Column(String(100), nullable=False)
    transaction_category = Column(String(255))
    transaction_mode = Column(String(100))
    transaction_purpose = Column(String(255))
    status = Column(String(100))
    account = relationship("Account", back_populates="transactions")
    file = relationship("FileInfo", back_populates="transactions")
    beneficiary = relationship("Beneficiary", back_populates="transactions")
    amount = relationship("TransactionAmount", uselist=False, back_populates="transaction")
    channel = relationship("TransactionChannel", uselist=False, back_populates="transaction")
    security = relationship("TransactionSecurity", uselist=False, back_populates="transaction")
    balance = relationship("Balance", uselist=False, back_populates="transaction")
    compliance = relationship("Compliance", uselist=False, back_populates="transaction")
    audit_log = relationship("AuditLog", uselist=False, back_populates="transaction")

class TransactionAmount(Base):
    __tablename__ = "transaction_amounts"
    transaction_id = Column(String(100), ForeignKey("transactions.transaction_id"), primary_key=True)
    transaction_amount = Column(String(255), nullable=False)
    currency_code = Column(String(100), nullable=False)
    converted_amount = Column(String(255))
    transaction_fees = Column(String(255))
    tax_amount = Column(String(255))
    net_amount = Column(String(255), nullable=False)
    transaction = relationship("Transaction", back_populates="amount")

class TransactionChannel(Base):
    __tablename__ = "transaction_channel"
    transaction_id = Column(String(100), ForeignKey("transactions.transaction_id"), primary_key=True)
    merchant_id = Column(String(100), ForeignKey("merchants.merchant_id"))
    channel_type = Column(String(100))
    device_id = Column(String(100))
    network_type = Column(String(100))
    latitude = Column(String(255))
    longitude = Column(String(255))
    transaction = relationship("Transaction", back_populates="channel")
    merchant = relationship("Merchant", back_populates="transaction_channels")

class TransactionSecurity(Base):
    __tablename__ = "transaction_security"
    transaction_id = Column(String(100), ForeignKey("transactions.transaction_id"), primary_key=True)
    authentication_method = Column(String(255))
    card_type = Column(String(255))
    risk_score = Column(String(255))
    fraud_flag = Column(String(255))
    transaction = relationship("Transaction", back_populates="security")

class Balance(Base):
    __tablename__ = "balances"
    transaction_id = Column(String(100), ForeignKey("transactions.transaction_id"), primary_key=True)
    opening_balance = Column(String(255))
    closing_balance = Column(String(255))
    available_balance = Column(String(255))
    transaction = relationship("Transaction", back_populates="balance")

class Compliance(Base):
    __tablename__ = "compliance"
    transaction_id = Column(String(100), ForeignKey("transactions.transaction_id"), primary_key=True)
    kyc_status = Column(String(255))
    risk_category = Column(String(255))
    aml_flag = Column(String(255))
    transaction = relationship("Transaction", back_populates="compliance")

class AuditLog(Base):
    __tablename__ = "audit_log"
    transaction_id = Column(String(100), ForeignKey("transactions.transaction_id"), primary_key=True)
    created_by = Column(String(255))
    processing_time_ms = Column(String(255))
    core_banking_module = Column(String(255))
    transaction = relationship("Transaction", back_populates="audit_log")

# --- SCHEMAS ---
class SummaryResponse(BaseModel):
    file_id: str
    total_transactions: int
    total_amount: float
    unique_customers: int
    transaction_type_distribution: Optional[Dict[str, int]] = None
    account_activity_summary: Optional[Dict[str, int]] = None

class AccountInformation(BaseModel):
    account_number: str
    account_type: str
    branch_code: str
    iban: Optional[str] = None
    swift_code: Optional[str] = None
    account_status: str
    account_open_date: str
    # Embedded customer fields
    customer_id: str
    account_holder_name: str
    mobile_number: Optional[str] = None
    email_id: Optional[str] = None
    pan_number: Optional[str] = None
    customer_segment: Optional[str] = None
    class Config: extra = "ignore"

class TransactionIdentification(BaseModel):
    transaction_id: str
    reference_number: str
    class Config: extra = "ignore"

class TransactionDetails(BaseModel):
    transaction_datetime: str
    transaction_type: str
    transaction_category: Optional[str] = None
    transaction_mode: Optional[str] = None
    transaction_purpose: Optional[str] = None
    class Config: extra = "ignore"

class AmountAndCurrency(BaseModel):
    transaction_amount: float
    currency_code: str
    converted_amount: Optional[float] = None
    transaction_fees: Optional[float] = None
    tax_amount: Optional[float] = None
    net_amount: float
    class Config: extra = "ignore"

class GeoCoordinates(BaseModel):
    latitude: float
    longitude: float
    class Config: extra = "ignore"

class ChannelInformation(BaseModel):
    merchant_id: Optional[str] = None
    merchant_name: Optional[str] = None
    channel_type: Optional[str] = None
    device_id: Optional[str] = None
    network_type: Optional[str] = None
    geo_coordinates: Optional[GeoCoordinates] = None
    class Config: extra = "ignore"

class BeneficiaryDetails(BaseModel):
    name: Optional[str] = Field(None, alias="beneficiary_name")
    account_number: Optional[str] = Field(None, alias="beneficiary_account_number")
    bank_name: Optional[str] = Field(None, alias="beneficiary_bank_name")
    city: Optional[str] = Field(None, alias="beneficiary_city")
    type: Optional[str] = Field(None, alias="beneficiary_type")
    class Config: extra = "ignore"; populate_by_name = True

class AuthorizationAndSecurity(BaseModel):
    authentication_method: Optional[str] = None
    card_type: Optional[str] = None
    risk_score: Optional[float] = None
    fraud_flag: Optional[bool] = False
    class Config: extra = "ignore"

class BalanceInformation(BaseModel):
    opening_balance: Optional[float] = None
    closing_balance: Optional[float] = None
    available_balance: Optional[float] = None
    class Config: extra = "ignore"

class TransactionStatus(BaseModel):
    status: Optional[str] = None
    class Config: extra = "ignore"

class ComplianceAndRegulatory(BaseModel):
    kyc_status: Optional[str] = None
    risk_category: Optional[str] = None
    aml_flag: Optional[bool] = False
    class Config: extra = "ignore"

class AuditAndOperational(BaseModel):
    created_by: Optional[str] = None
    processing_time_ms: Optional[int] = None
    core_banking_module: Optional[str] = None
    class Config: extra = "ignore"

class TransactionEvent(BaseModel):
    transaction_identification: TransactionIdentification
    account_information: AccountInformation
    transaction_details: TransactionDetails
    amount_and_currency: AmountAndCurrency
    channel_information: Optional[ChannelInformation] = None
    beneficiary_details: Optional[BeneficiaryDetails] = None
    authorization_and_security: Optional[AuthorizationAndSecurity] = None
    balance_information: Optional[BalanceInformation] = None
    transaction_status: Optional[TransactionStatus] = None
    compliance_and_regulatory: Optional[ComplianceAndRegulatory] = None
    audit_and_operational: Optional[AuditAndOperational] = None
    class Config: extra = "ignore"; populate_by_name = True

class DateRange(BaseModel):
    from_date: str = Field(..., alias="from")
    to_date: str = Field(..., alias="to")
    class Config: extra = "ignore"; populate_by_name = True

class BatchMetadata(BaseModel):
    batch_id: str = Field(..., alias="batch")
    batch_name: Optional[str] = Field("Untitled Batch", alias="batch_name")
    generated_time: str = Field(..., alias="generated_at")
    date_range: Optional[DateRange] = None
    class Config: extra = "ignore"; populate_by_name = True

class FileUploadPayload(BaseModel):
    batch_metadata: Optional[BatchMetadata] = None
    transactions: List[TransactionEvent] = Field(..., alias="retail_banking_transactions")
    
    @model_validator(mode='before')
    @classmethod
    def wrap_metadata(cls, data: Any) -> Any:
        if isinstance(data, dict):
            # Support both keys for transactions
            tx_key = 'retail_banking_transactions' if 'retail_banking_transactions' in data else 'transactions'
            if tx_key in data and 'batch_metadata' not in data:
                meta = {k: data[k] for k in ['batch', 'batch_id', 'generated_at', 'generated_time', 'date_range'] if k in data}
                if meta: data['batch_metadata'] = meta
        return data
    class Config: extra = "ignore"; populate_by_name = True

# --- LOGIC ---
async def upsert_bulk(session, model, data, conflict_target, update_cols=None, batch_size=500):
    if not data: return
    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        stmt = insert(model).values(batch)
        if update_cols:
            set_dict = {col: stmt.excluded[col] for col in update_cols}
            stmt = stmt.on_conflict_do_update(index_elements=[conflict_target], set_=set_dict)
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=[conflict_target])
        await session.execute(stmt)

def parse_payload_for_db(payload: FileUploadPayload):
    tables = {"files": [], "customers": [], "accounts": [], "merchants": [], "beneficiary": [], "transactions": [], "amounts": [], "channels": [], "security": [], "balances": [], "compliance": [], "audit": []}
    meta = payload.batch_metadata
    
    # Ensure meta is not None to avoid crashes
    batch_id = meta.batch_id if meta else f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    batch_name = meta.batch_name if meta else "Manual Upload"
    generated_time = meta.generated_time if meta else datetime.now().isoformat()
    from_date = meta.date_range.from_date if meta and meta.date_range else None
    to_date = meta.date_range.to_date if meta and meta.date_range else None

    tables["files"].append({
        "file_id": batch_id, 
        "batch_name": batch_name, 
        "total_records": str(len(payload.transactions)), 
        "generated_at": str(generated_time), 
        "from_date": from_date, 
        "to_date": to_date
    })
    
    seen = {"c": set(), "a": set(), "m": set(), "b": set()}
    for txn in payload.transactions:
        a = txn.account_information
        if a.customer_id not in seen["c"]:
            tables["customers"].append({
                "customer_id": a.customer_id, 
                "account_holder_name": a.account_holder_name, 
                "mobile_number": str(a.mobile_number) if a.mobile_number else None, 
                "email_id": a.email_id, 
                "pan_number": a.pan_number, 
                "customer_segment": a.customer_segment
            })
            seen["c"].add(a.customer_id)
        
        if a.account_number not in seen["a"]:
            tables["accounts"].append({
                "account_number": a.account_number, 
                "customer_id": a.customer_id, 
                "account_type": a.account_type, 
                "branch_code": a.branch_code, 
                "iban": a.iban, 
                "swift_code": a.swift_code, 
                "account_status": a.account_status, 
                "account_open_date": str(a.account_open_date)
            })
            seen["a"].add(a.account_number)
            
        tid = txn.transaction_identification.transaction_id
        
        # Safe access for channel information
        ch = txn.channel_information
        m_id = ch.merchant_id if ch else None
        if m_id and m_id not in seen["m"]:
            tables["merchants"].append({
                "merchant_id": m_id, 
                "merchant_name": ch.merchant_name or ch.channel_type or "N/A", 
                "merchant_category_code": "N/A", 
                "location": "N/A"
            })
            seen["m"].add(m_id)
            
        # Safe access for beneficiary details
        ben_id = None
        if txn.beneficiary_details:
            ben = txn.beneficiary_details
            if ben.account_number and ben.name:
                ben_id = f"BEN_{ben.account_number}_{ben.name}"
                if ben_id not in seen["b"]:
                    tables["beneficiary"].append({
                        "beneficiary_id": ben_id, 
                        "name": ben.name, 
                        "account_number": ben.account_number, 
                        "bank_name": ben.bank_name, 
                        "city": ben.city, 
                        "type": ben.type
                    })
                    seen["b"].add(ben_id)
                    
        # Main transaction record
        tables["transactions"].append({
            "transaction_id": tid, 
            "account_number": a.account_number, 
            "file_id": batch_id, 
            "beneficiary_id": ben_id, 
            "reference_number": str(txn.transaction_identification.reference_number), 
            "transaction_datetime": str(txn.transaction_details.transaction_datetime), 
            "transaction_type": txn.transaction_details.transaction_type, 
            "transaction_category": txn.transaction_details.transaction_category, 
            "transaction_mode": txn.transaction_details.transaction_mode, 
            "transaction_purpose": txn.transaction_details.transaction_purpose, 
            "status": txn.transaction_status.status if txn.transaction_status else "COMPLETED"
        })
        
        # Transaction amount
        amt = txn.amount_and_currency
        tables["amounts"].append({
            "transaction_id": tid, 
            "transaction_amount": str(amt.transaction_amount), 
            "currency_code": amt.currency_code, 
            "converted_amount": str(amt.converted_amount) if amt.converted_amount else None, 
            "transaction_fees": str(amt.transaction_fees) if amt.transaction_fees else None, 
            "tax_amount": str(amt.tax_amount) if amt.tax_amount else None, 
            "net_amount": str(amt.net_amount)
        })
        
        # Optional: Channel details
        if ch:
            tables["channels"].append({
                "transaction_id": tid, 
                "merchant_id": m_id, 
                "channel_type": ch.channel_type, 
                "device_id": ch.device_id, 
                "network_type": ch.network_type, 
                "latitude": str(ch.geo_coordinates.latitude) if ch.geo_coordinates else None, 
                "longitude": str(ch.geo_coordinates.longitude) if ch.geo_coordinates else None
            })
            
        # Optional: Security details
        if txn.authorization_and_security:
            sec = txn.authorization_and_security
            tables["security"].append({
                "transaction_id": tid, 
                "authentication_method": sec.authentication_method, 
                "card_type": sec.card_type, 
                "risk_score": str(sec.risk_score) if sec.risk_score else None, 
                "fraud_flag": str(sec.fraud_flag)
            })
            
        # Optional: Balance details
        if txn.balance_information:
            bal = txn.balance_information
            tables["balances"].append({
                "transaction_id": tid, 
                "opening_balance": str(bal.opening_balance) if bal.opening_balance is not None else None, 
                "closing_balance": str(bal.closing_balance) if bal.closing_balance is not None else None, 
                "available_balance": str(bal.available_balance) if bal.available_balance is not None else None
            })
            
        # Optional: Compliance details
        if txn.compliance_and_regulatory:
            comp = txn.compliance_and_regulatory
            tables["compliance"].append({
                "transaction_id": tid, 
                "kyc_status": comp.kyc_status, 
                "risk_category": comp.risk_category, 
                "aml_flag": str(comp.aml_flag)
            })
            
        # Optional: Audit details
        if txn.audit_and_operational:
            audit = txn.audit_and_operational
            tables["audit"].append({
                "transaction_id": tid, 
                "created_by": audit.created_by, 
                "processing_time_ms": str(audit.processing_time_ms), 
                "core_banking_module": audit.core_banking_module
            })
    return tables

# --- API ---
app = FastAPI(title="Retail Banking Transaction API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    logger.error(f"Validation Error: {exc.errors()}")
    return HTTPException(status_code=422, detail=exc.errors())

@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

@app.post("/upload")
async def upload_transactions(payload: FileUploadPayload, db: AsyncSession = Depends(get_db)):
    try:
        tables = parse_payload_for_db(payload)
        await upsert_bulk(db, FileInfo, tables["files"], "file_id")
        await upsert_bulk(db, Customer, tables["customers"], "customer_id", ["account_holder_name", "mobile_number", "email_id", "pan_number", "customer_segment"])
        await upsert_bulk(db, Account, tables["accounts"], "account_number", ["account_type", "branch_code", "iban", "swift_code", "account_status", "account_open_date"])
        await upsert_bulk(db, Merchant, tables["merchants"], "merchant_id", ["merchant_name", "merchant_category_code", "location"])
        await upsert_bulk(db, Beneficiary, tables["beneficiary"], "beneficiary_id", ["name", "account_number", "bank_name", "city", "type"])
        await upsert_bulk(db, Transaction, tables["transactions"], "transaction_id")
        await upsert_bulk(db, TransactionAmount, tables["amounts"], "transaction_id")
        await upsert_bulk(db, TransactionChannel, tables["channels"], "transaction_id")
        await upsert_bulk(db, TransactionSecurity, tables["security"], "transaction_id")
        await upsert_bulk(db, Balance, tables["balances"], "transaction_id")
        await upsert_bulk(db, Compliance, tables["compliance"], "transaction_id")
        await upsert_bulk(db, AuditLog, tables["audit"], "transaction_id")
        await db.commit()
        return {"status": "success", "file_id": payload.batch_metadata.batch_id}
    except Exception as e:
        await db.rollback()
        logger.error(f"Upload failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/summary/overall")
async def overall_summary(db: AsyncSession = Depends(get_db)):
    try:
        tx_count = (await db.execute(select(func.count(Transaction.transaction_id)))).scalar() or 0
        total_vol = (await db.execute(select(func.sum(cast(TransactionAmount.transaction_amount, Numeric))))).scalar() or 0.0
        top_merch_stmt = select(Merchant.merchant_name, func.sum(cast(TransactionAmount.transaction_amount, Numeric)).label('vol')).join(TransactionChannel, Merchant.merchant_id == TransactionChannel.merchant_id).join(TransactionAmount, TransactionChannel.transaction_id == TransactionAmount.transaction_id).group_by(Merchant.merchant_id, Merchant.merchant_name).order_by(desc('vol')).limit(5)
        merch_rows = await db.execute(top_merch_stmt)
        top_merchants = [{"name": row[0], "volume": float(row[1])} for row in merch_rows.all()]
        dna_stmt = select(Transaction.transaction_type, func.count(Transaction.transaction_id)).group_by(Transaction.transaction_type)
        dna_rows = await db.execute(dna_stmt)
        dna = {row[0]: row[1] for row in dna_rows.all()}
        return {"total_transactions": tx_count, "total_volume": float(total_vol), "top_merchants": top_merchants, "account_activity_summary": dna}
    except Exception as e:
        logger.error(f"Summary failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/summary/file/{file_id}", response_model=SummaryResponse)
async def file_summary(file_id: str, db: AsyncSession = Depends(get_db)):
    try:
        tx_count = (await db.execute(select(func.count(Transaction.transaction_id)).where(Transaction.file_id == file_id))).scalar() or 0
        total_vol = (await db.execute(select(func.sum(cast(TransactionAmount.transaction_amount, Numeric))).where(Transaction.file_id == file_id))).scalar() or 0.0
        unique_cust = (await db.execute(select(func.count(func.distinct(Account.customer_id))).join(Transaction).where(Transaction.file_id == file_id))).scalar() or 0
        dna_stmt = select(Transaction.transaction_type, func.count(Transaction.transaction_id)).where(Transaction.file_id == file_id).group_by(Transaction.transaction_type)
        dna_rows = await db.execute(dna_stmt)
        dna = {row[0]: row[1] for row in dna_rows.all()}
        return {"file_id": file_id, "total_transactions": tx_count, "total_amount": float(total_vol), "unique_customers": unique_cust, "transaction_type_distribution": dna}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files")
async def list_files(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(FileInfo.file_id))
    return [row[0] for row in result.all()]

@app.post("/clear")
async def clear_database(db: AsyncSession = Depends(get_db)):
    try:
        # Delete related tables first (FK constraints)
        models = [
            AuditLog, Compliance, Balance, TransactionSecurity, 
            TransactionChannel, TransactionAmount, Transaction, 
            Beneficiary, Merchant, Account, Customer, FileInfo
        ]
        for model in models:
            await db.execute(delete(model))
        await db.commit()
        return {"status": "success", "message": "Ecosystem Reset Complete"}
    except Exception as e:
        await db.rollback()
        logger.error(f"Clear failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
