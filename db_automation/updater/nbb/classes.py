from dataclasses import dataclass
from datetime import date


@dataclass
class ExerciseDates:
    start_date: date
    end_date: date

    @classmethod
    def from_dict(cls, data: dict) -> "ExerciseDates":
        return cls(
            start_date=date.fromisoformat(data["StartDate"]),
            end_date=date.fromisoformat(data["EndDate"]),
        )


@dataclass
class ReferenceAddress:
    street: str
    number: str
    postal_code: str
    city: str
    country_code: str
    box: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "ReferenceAddress":
        return cls(
            street=data["Street"],
            number=data["Number"],
            postal_code=data["PostalCode"],
            city=data["City"],
            country_code=data["CountryCode"],
            box=data.get("Box"),
        )


@dataclass
class Reference:
    reference_number: str
    enterprise_number: str
    enterprise_name: str | None
    deposit_date: date
    exercise_dates: ExerciseDates
    deposit_type: str
    model_type: str
    language: str
    currency: str | None
    address: ReferenceAddress
    legal_form: str
    legal_situation: str | None
    legal_validation: bool
    activity_code: str | None
    account_data_url: str
    data_version: str | None
    assembly_date: date | None = None
    improvement_date: date | None = None
    corrected_data: bool | None = None

    @property
    def account_year(self) -> int:
        return self.exercise_dates.end_date.year

    @classmethod
    def from_dict(cls, data: dict) -> "Reference":
        return cls(
            reference_number=data["ReferenceNumber"],
            enterprise_number=data["EnterpriseNumber"],
            enterprise_name=data["EnterpriseName"],
            deposit_date=date.fromisoformat(data["DepositDate"]),
            exercise_dates=ExerciseDates.from_dict(data["ExerciseDates"]),
            deposit_type=data["DepositType"],
            model_type=data["ModelType"],
            language=data["Language"],
            currency=data["Currency"],
            address=ReferenceAddress.from_dict(data["Address"]),
            legal_form=data["LegalForm"],
            legal_situation=data["LegalSituation"],
            legal_validation=data["FullFillLegalValidation"],
            activity_code=data["ActivityCode"],
            account_data_url=data["AccountingDataURL"],
            data_version=data["DataVersion"],
            assembly_date=date.fromisoformat(d) if (d := data.get("GeneralAssemblyDate")) else None,
            improvement_date=date.fromisoformat(d) if (d := data.get("ImprovementDate")) else None,
            corrected_data=data.get("CorrectedData"),
        )
