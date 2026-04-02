from datetime import date, datetime

from sqlalchemy import ForeignKey, String, Integer, Boolean
from sqlalchemy.orm import DeclarativeBase, Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.dialects.postgresql import JSON, TIMESTAMP, DATE


class Base(DeclarativeBase):
    pass


class CountryCode(Base):
    __tablename__ = "country_codes"

    english_name: Mapped[str] = mapped_column(String(63), nullable=True)
    dutch_name: Mapped[str] = mapped_column(
        String(63),
        primary_key=True,
        nullable=False,
    )
    a_2: Mapped[str] = mapped_column(String(2), nullable=False)
    a_3: Mapped[str] = mapped_column(String(3), nullable=False)
    numeric_code: Mapped[str] = mapped_column(String(3), nullable=False)
    iso_3166_2: Mapped[str] = mapped_column(String(13), nullable=False)

    def __repr__(self) -> str:
        return f"{self.dutch_name}"


class CompanyInfo(Base):
    __tablename__ = "company_info"

    enterprise_id: Mapped[str] = mapped_column(String(10), primary_key=True)
    denomination: Mapped[str] = mapped_column(String(255), nullable=True)
    legal_situation: Mapped[str] = mapped_column(String(3), nullable=True)
    last_update: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False)

    def __repr__(self) -> str:
        return f"{self.enterprise_id}"


class Statement(Base):
    __tablename__ = "statements"

    filing_id: Mapped[str] = mapped_column(String(15), primary_key=True)
    enterprise_id: Mapped[str] = mapped_column(String(10), nullable=False)
    start_date: Mapped[date] = mapped_column(DATE, nullable=False)
    end_date: Mapped[date] = mapped_column(DATE, nullable=False)
    account_year: Mapped[int] = mapped_column(Integer,  nullable=False)
    deposit_date: Mapped[date] = mapped_column(DATE, nullable=False)
    deposit_type: Mapped[str] = mapped_column(String(15), nullable=False)
    currency: Mapped[str] = mapped_column(String(7))
    legal_form: Mapped[str] = mapped_column(String(3), nullable=False)
    activity_code: Mapped[str] = mapped_column(String(5))
    model_type: Mapped[str] = mapped_column(String(10), nullable=False)
    account_url: Mapped[bool] = mapped_column(Boolean, nullable=False)
    legal_validation: Mapped[bool] = mapped_column(Boolean)
    assembly_date: Mapped[date] = mapped_column(DATE)
    data_version: Mapped[str] = mapped_column(String(31))
    improvement_date: Mapped[date] = mapped_column(DATE)
    corrected_data: Mapped[bool] = mapped_column(Boolean)
    last_update: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False)

    def __repr__(self) -> str:
        return f"{self.enterprise_id}-{self.filing_id}"


# class Filing(Base):
#     __tablename__ = "updated_filings"

#     enterprise_id: Mapped[str] = mapped_column(
#         ForeignKey(
#             "updated_references.enterprise_id",
#             ondelete="CASCADE"
#         ),
#         primary_key=True,
#         nullable=False,
#     )
#     filing_id: Mapped[str] = mapped_column(
#         primary_key=True,
#         nullable=False,
#     )
#     json_filing: Mapped[dict | None] = mapped_column(JSON)
#     last_update: Mapped[datetime | None] = mapped_column(TIMESTAMP)
