import uuid
from sqlalchemy import Column, String, TIMESTAMP, JSON, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from app.db import Base


class TransactionRuleExecution(Base):
    __tablename__ = "transaction_rule_execution"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    transaction_id = Column(UUID(as_uuid=True), ForeignKey("transactions.id"))
    rule_id = Column(UUID(as_uuid=True), ForeignKey("rules.id"))

    result = Column(String(20))  # SUCCESS / SKIPPED / FAILED
    details = Column(JSON)

    executed_at = Column(TIMESTAMP, server_default=func.now())
