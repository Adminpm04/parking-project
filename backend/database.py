from sqlalchemy import create_engine, Column, String, Float, DateTime, Enum, Boolean, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import enum
from config import settings

engine = create_engine(settings.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class OpenType(str, enum.Enum):
    auto = "auto"       # camera + payment
    manual = "manual"   # guard with button (cash)

class PaymentStatus(str, enum.Enum):
    pending = "pending"
    paid = "paid"
    free = "free"       # abonement or VIP

class ParkingSession(Base):
    __tablename__ = "parking_sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plate = Column(String(20), nullable=True)
    entry_time = Column(DateTime, default=datetime.utcnow)
    exit_time = Column(DateTime, nullable=True)
    entry_type = Column(Enum(OpenType), default=OpenType.auto)
    exit_type = Column(Enum(OpenType), nullable=True)
    payment_status = Column(Enum(PaymentStatus), default=PaymentStatus.pending)
    amount = Column(Float, nullable=True)
    invoice_id = Column(String(100), nullable=True)
    mis_payment_id = Column(String(100), nullable=True)
    phone_number = Column(String(20), nullable=True)
    is_active = Column(Boolean, default=True)

class Abonement(Base):
    __tablename__ = "abonements"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plate = Column(String(20), unique=True, nullable=False)
    owner_name = Column(String(100), nullable=True)
    valid_until = Column(DateTime, nullable=False)
    is_active = Column(Boolean, default=True)

class Blacklist(Base):
    __tablename__ = "blacklist"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plate = Column(String(20), unique=True, nullable=False)
    reason = Column(String(200), nullable=True)
    added_at = Column(DateTime, default=datetime.utcnow)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    Base.metadata.create_all(bind=engine)
