from datetime import datetime

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import DeclarativeBase, Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.dialects.postgresql import JSON, TIMESTAMP


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    fullname: Mapped[str]


class Reference(Base):
    __tablename__ = "updated_references"

    enterprise_id: Mapped[str] = mapped_column(
        String(10),
        primary_key=True,
        nullable=False,
    )
    json_reference: Mapped[list | None] = mapped_column(JSON)
    last_update: Mapped[datetime | None] = mapped_column(TIMESTAMP)

    def __repr__(self) -> str:
        return f"{self.enterprise_id}"


class Filing(Base):
    __tablename__ = "updated_filings"

    enterprise_id: Mapped[str] = mapped_column(
        ForeignKey(
            "updated_references.enterprise_id",
            ondelete="CASCADE"
        ),
        primary_key=True,
        nullable=False,
    )
    filing_id: Mapped[str] = mapped_column(
        primary_key=True,
        nullable=False,
    )
    json_filing: Mapped[dict | None] = mapped_column(JSON)
    last_update: Mapped[datetime | None] = mapped_column(TIMESTAMP)
