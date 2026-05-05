import uuid
from datetime import date, datetime

from sqlalchemy import BigInteger, Float, ForeignKey, Integer, String, Boolean
from sqlalchemy.orm import DeclarativeBase, Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.dialects.postgresql import JSON, TIMESTAMP, DATE, UUID


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
    search_field: Mapped[str] = mapped_column(String(255), nullable=True)
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


class AccountingCode(Base):
    __tablename__ = "accounting_codes"

    accountcode_id: Mapped[str] = mapped_column(String(7), primary_key=True)
    denomination: Mapped[str] = mapped_column(String(255), nullable=False)

    def __repr__(self) -> str:
        return f"{self.denomination}"


class StatementFact(Base):
    __tablename__ = "statement_facts"

    account_year: Mapped[int] = mapped_column(Integer, primary_key=True)
    filing_id: Mapped[str] = mapped_column(
        ForeignKey("statements.filing_id"), primary_key=True
    )
    accountcode_id: Mapped[str] = mapped_column(
        ForeignKey("accounting_codes.accountcode_id"), primary_key=True
    )
    book_value: Mapped[float] = mapped_column(Float, nullable=True)
    previous_value: Mapped[float] = mapped_column(Float, nullable=True)

    def __repr__(self) -> str:
        return f"{self.filing_id}-{self.accountcode_id}"


class NaturalPerson(Base):
    __tablename__ = "natural_persons"

    person_uuid: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True
    )
    first_name: Mapped[str] = mapped_column(String(63), nullable=False)
    last_name: Mapped[str] = mapped_column(String(63), nullable=False)
    street: Mapped[str] = mapped_column(String(255), nullable=True)
    street_number: Mapped[str] = mapped_column(String(255), nullable=True)
    box: Mapped[str] = mapped_column(String(255), nullable=True)
    zipcode: Mapped[str] = mapped_column(String(255), nullable=True)
    country_code: Mapped[str] = mapped_column(String(2), nullable=True)
    search_name: Mapped[str] = mapped_column(String(255), nullable=True)

    def __repr__(self) -> str:
        return f"{self.first_name} {self.last_name}"


class Entity(Base):
    __tablename__ = "entities"

    entity_uuid: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True
    )
    entity_id: Mapped[str] = mapped_column(String(39), nullable=False)
    country_code: Mapped[str] = mapped_column(String(2), nullable=False)
    denomination: Mapped[str] = mapped_column(String(255), nullable=True)
    street: Mapped[str] = mapped_column(String(255), nullable=True)
    street_number: Mapped[str] = mapped_column(String(255), nullable=True)
    box: Mapped[str] = mapped_column(String(255), nullable=True)
    zipcode: Mapped[str] = mapped_column(String(255), nullable=True)

    def __repr__(self) -> str:
        return f"{self.entity_id} - {self.denomination}"


class AdministratorNatural(Base):
    __tablename__ = "administrators_natural"

    enterprise_id: Mapped[str] = mapped_column(
        ForeignKey("company_info.enterprise_id"), primary_key=True
    )
    person_uuid: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("natural_persons.person_uuid"), primary_key=True
    )
    filing_id: Mapped[str] = mapped_column(
        ForeignKey("statements.filing_id"), primary_key=True
    )
    account_year: Mapped[int] = mapped_column(Integer, primary_key=True)

    def __repr__(self) -> str:
        return f"{self.enterprise_id}-{self.filing_id}"


class AdministratorLegal(Base):
    __tablename__ = "administrators_legal"

    enterprise_id: Mapped[str] = mapped_column(
        ForeignKey("company_info.enterprise_id"), primary_key=True
    )
    entity_uuid: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("entities.entity_uuid"), primary_key=True
    )
    person_uuid: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("natural_persons.person_uuid"), primary_key=True
    )
    filing_id: Mapped[str] = mapped_column(
        ForeignKey("statements.filing_id"), primary_key=True
    )
    account_year: Mapped[int] = mapped_column(Integer, primary_key=True)

    def __repr__(self) -> str:
        return f"{self.enterprise_id}-{self.filing_id}"


class Mandate(Base):
    __tablename__ = "mandates"

    person_uuid: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("natural_persons.person_uuid"), primary_key=True
    )
    enterprise_id: Mapped[str] = mapped_column(
        ForeignKey("company_info.enterprise_id"), primary_key=True
    )
    filing_id: Mapped[str] = mapped_column(
        ForeignKey("statements.filing_id"), nullable=False
    )
    function_code: Mapped[str] = mapped_column(String(7), primary_key=True)
    start_date: Mapped[date] = mapped_column(DATE, nullable=True)
    end_date: Mapped[date] = mapped_column(DATE, nullable=True)
    account_year: Mapped[int] = mapped_column(Integer, primary_key=True)

    def __repr__(self) -> str:
        return f"{self.enterprise_id}-{self.function_code}"


class ParticipatingInterest(Base):
    __tablename__ = "participating_interests"

    enterprise_id: Mapped[str] = mapped_column(
        ForeignKey("company_info.enterprise_id"), primary_key=True
    )
    entity_uuid: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("entities.entity_uuid"), primary_key=True
    )
    filing_id: Mapped[str] = mapped_column(
        ForeignKey("statements.filing_id"), primary_key=True
    )
    account_year: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_date: Mapped[date] = mapped_column(DATE, nullable=True)
    currency: Mapped[str] = mapped_column(String(15), primary_key=True)
    equity: Mapped[int] = mapped_column(BigInteger, nullable=True)
    net_result: Mapped[int] = mapped_column(BigInteger, nullable=True)
    nature: Mapped[str] = mapped_column(String(255), primary_key=True)
    line: Mapped[str] = mapped_column(String(63), primary_key=True)
    amount: Mapped[int] = mapped_column(BigInteger, nullable=True)
    percentage_held: Mapped[float] = mapped_column(Float, nullable=True)
    percentage_subsidiary: Mapped[float] = mapped_column(Float, nullable=True)

    def __repr__(self) -> str:
        return f"{self.enterprise_id}-{self.entity_uuid}"


class Shareholder(Base):
    __tablename__ = "shareholders"

    enterprise_id: Mapped[str] = mapped_column(
        ForeignKey("company_info.enterprise_id"), primary_key=True
    )
    entity_uuid: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("entities.entity_uuid"), primary_key=True
    )
    person_uuid: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("natural_persons.person_uuid"), primary_key=True
    )
    filing_id: Mapped[str] = mapped_column(
        ForeignKey("statements.filing_id"), primary_key=True
    )
    account_year: Mapped[int] = mapped_column(Integer, primary_key=True)
    nature_rights: Mapped[str] = mapped_column(String(255), nullable=True)
    line_rights: Mapped[str] = mapped_column(String(3), nullable=True)
    securities_attached: Mapped[int] = mapped_column(Integer, nullable=True)
    not_securities_attached: Mapped[str] = mapped_column(String(7), nullable=True)
    percentage: Mapped[float] = mapped_column(Float, nullable=True)

    def __repr__(self) -> str:
        return f"{self.enterprise_id}-{self.account_year}"
