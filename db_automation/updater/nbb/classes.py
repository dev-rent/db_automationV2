import uuid
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from rapidfuzz import fuzz


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


# -------------------------- Filings Dataclasses ------------------------------
@dataclass
class EnterpriseAddress:
    street: str | None
    number: str | None
    box: str | None
    city: str | None
    country: str | None
    other_postal_code: str | None = None
    other_city: str | None = None
    other_country: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "EnterpriseAddress":
        return cls(
            street=data.get("Street"),
            number=data.get("Number"),
            box=data.get("Box"),
            city=data.get("City"),
            country=data.get("Country"),
            other_postal_code=data.get("OtherPostalCode"),
            other_city=data.get("OtherCity"),
            other_country=data.get("OtherCountry"),
        )


@dataclass
class PersonAddress:
    street: str | None
    number: str | None
    box: str | None
    city: str | None
    country: str | None
    address_type: str | None = None
    other_postal_code: str | None = None
    other_city: str | None = None
    other_country: str | None = None
    other_address_type: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "PersonAddress":
        return cls(
            street=data.get("Street"),
            number=data.get("Number"),
            box=data.get("Box"),
            city=data.get("City"),
            country=data.get("Country"),
            address_type=data.get("AddressType"),
            other_postal_code=data.get("OtherPostalCode"),
            other_city=data.get("OtherCity"),
            other_country=data.get("OtherCountry"),
            other_address_type=data.get("OtherAddressType"),
        )


@dataclass
class LegalForm:
    code: str | None
    denomination: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "LegalForm":
        return cls(
            code=data.get("LegalFormCode"),
            denomination=data.get("LegalForm"),
        )


@dataclass
class JointCommittee:
    code: str
    denomination: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "JointCommittee":
        return cls(
            code=data["JointCommitteeCode"],
            denomination=data.get("JointCommittee"),
        )


@dataclass
class Rubric:
    code: str
    value: str
    period: str
    data_type: str
    type_amount: str

    @classmethod
    def from_dict(cls, data: dict) -> "Rubric":
        return cls(
            code=data["Code"],
            value=data["Value"],
            period=data["Period"],
            data_type=data["DataType"],
            type_amount=data["TypeAmount"],
        )


@dataclass
class MandateDates:
    start_date: date | None = None
    end_date: date | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "MandateDates":
        return cls(
            start_date=date.fromisoformat(d) if (d := data.get("StartDate")) else None,
            end_date=date.fromisoformat(d) if (d := data.get("EndDate")) else None,
        )


@dataclass
class Mandate:
    function_code: str | None
    mandate_dates: MandateDates
    other_function_code: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "Mandate":
        return cls(
            function_code=data.get("FunctionMandate"),
            mandate_dates=MandateDates.from_dict(data.get("MandateDates", {})),
            other_function_code=data.get("OtherFunctionMandate"),
        )


@dataclass
class NaturalPerson:
    first_name: str
    last_name: str
    address: PersonAddress
    mandates: list[Mandate]
    profession: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "NaturalPerson":
        person = data["Person"]
        return cls(
            first_name=person["FirstName"],
            last_name=person["LastName"],
            address=PersonAddress.from_dict(person.get("Address", {})),
            mandates=[Mandate.from_dict(m) for m in data.get("Mandates", [])],
            profession=data.get("Profession"),
        )


@dataclass
class Entity:
    name: str | None
    identifier: str | None
    identifier_type_code: str | None
    address: EnterpriseAddress

    @classmethod
    def from_dict(cls, data: dict) -> "Entity":
        id_kind = data.get("IdentifierKindOfNumber") or {}
        return cls(
            name=data.get("Name"),
            identifier=data.get("Identifier"),
            identifier_type_code=id_kind.get("IdentifierKindOfNumberCode"),
            address=EnterpriseAddress.from_dict(data.get("Address") or {}),
        )


@dataclass
class Representative:
    first_name: str
    last_name: str
    address: PersonAddress
    profession: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "Representative":
        return cls(
            first_name=data["FirstName"],
            last_name=data["LastName"],
            address=PersonAddress.from_dict(data.get("Address") or {}),
            profession=data.get("Profession"),
        )


@dataclass
class LegalPerson:
    entity: Entity
    mandates: list[Mandate]
    representatives: list[Representative]

    @classmethod
    def from_dict(cls, data: dict) -> "LegalPerson":
        return cls(
            entity=Entity.from_dict(data["Entity"]),
            mandates=[Mandate.from_dict(m) for m in data.get("Mandates", [])],
            representatives=[Representative.from_dict(r) for r in data.get("Representatives", [])],
        )


@dataclass
class InterestHeld:
    line: str
    nature: str | None
    number: str | None
    percentage_directly_held: str | None
    percentage_subsidiaries: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "InterestHeld":
        return cls(
            line=data["Line"],
            nature=data.get("Nature"),
            number=data.get("Number"),
            percentage_directly_held=data.get("PercentageDirectlyHeld"),
            percentage_subsidiaries=data.get("PercentageSubsidiaries"),
        )


@dataclass
class ParticipatingInterest:
    entity: Entity
    interests_held: list[InterestHeld]
    entity_legal_form: str | None = None  # encoded, e.g. "lgf:m014"
    other_legal_form: str | None = None
    account_date: date | None = None
    currency: str | None = None           # encoded, e.g. "ccy:mEUR"
    equity: str | None = None
    net_result: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "ParticipatingInterest":
        return cls(
            entity=Entity.from_dict(data["Entity"]),
            interests_held=[InterestHeld.from_dict(i) for i in data.get("ParticipatingInterestHeld", [])],
            entity_legal_form=data.get("EntityLegalForm"),
            other_legal_form=data.get("OtherLegalForm"),
            account_date=date.fromisoformat(d) if (d := data.get("AccountDate")) else None,
            currency=data.get("Currency"),
            equity=data.get("Equity"),
            net_result=data.get("NetResult"),
        )


@dataclass
class RightsHeld:
    line: str
    nature: str | None
    number_securities_attached: str | None
    number_not_securities_attached: str | None
    percentage: str | None

    @classmethod
    def from_dict(cls, data: dict) -> "RightsHeld":
        return cls(
            line=data["Line"],
            nature=data.get("Nature"),
            number_securities_attached=data.get("NumberSecuritiesAttached"),
            number_not_securities_attached=data.get("NumberNotSecuritiesAttached"),
            percentage=data.get("Percentage"),
        )


@dataclass
class EntityShareholder:
    entity: Entity
    rights_held: list[RightsHeld]

    @classmethod
    def from_dict(cls, data: dict) -> "EntityShareholder":
        return cls(
            entity=Entity.from_dict(data["Entity"]),
            rights_held=[RightsHeld.from_dict(r) for r in data.get("RightsHeld", [])],
        )


@dataclass
class IndividualShareholder:
    first_name: str
    last_name: str
    address: PersonAddress
    rights_held: list[RightsHeld]

    @classmethod
    def from_dict(cls, data: dict) -> "IndividualShareholder":
        person = data["Person"]
        return cls(
            first_name=person["FirstName"],
            last_name=person["LastName"],
            address=PersonAddress.from_dict(person.get("Address") or {}),
            rights_held=[RightsHeld.from_dict(r) for r in data.get("RightsHeld", [])],
        )


@dataclass
class Filing:
    reference_number: str
    enterprise_name: str | None
    account_year: int
    address: EnterpriseAddress
    legal_form: LegalForm
    joint_committees: list[JointCommittee]
    rubrics: list[Rubric]
    natural_persons: list[NaturalPerson]
    legal_persons: list[LegalPerson]
    participating_interests: list[ParticipatingInterest]
    shareholders_entity: list[EntityShareholder]
    shareholders_individual: list[IndividualShareholder]

    @classmethod
    def from_dict(cls, data: dict, account_year) -> "Filing":
        administrators = data.get("Administrators", {})
        shareholders = data.get("Shareholders", {})
        return cls(
            reference_number=data["ReferenceNumber"],
            enterprise_name=data.get("EnterpriseName"),
            account_year=account_year,
            address=EnterpriseAddress.from_dict(data.get("Address") or {}),
            legal_form=LegalForm.from_dict(data.get("LegalForm") or {}),
            joint_committees=[JointCommittee.from_dict(jc) for jc in data.get("JointCommittees", [])],
            rubrics=[Rubric.from_dict(r) for r in data.get("Rubrics", [])],
            natural_persons=[NaturalPerson.from_dict(p) for p in administrators.get("NaturalPersons", [])],
            legal_persons=[LegalPerson.from_dict(lp) for lp in administrators.get("LegalPersons", [])],
            participating_interests=[ParticipatingInterest.from_dict(pi) for pi in data.get("ParticipatingInterests", [])],
            shareholders_entity=[EntityShareholder.from_dict(s) for s in shareholders.get("EntityShareHolders", [])],
            shareholders_individual=[IndividualShareholder.from_dict(s) for s in shareholders.get("IndividualShareHolders", [])],
        )


# -------------------------- Company Dataclasses ------------------------------

def _person_search_key(first_name: str, last_name: str) -> tuple[str, str]:
    fn = first_name.casefold().replace(" ", "")
    ln = last_name.casefold().replace(" ", "")
    return (fn + ln + fn, ln + fn + ln)


def _fuzzy_threshold(s: str) -> int:
    if len(s) <= 5:
        return 85
    if len(s) > 20:
        return 95
    return 90


@dataclass
class CompanyFiling:
    reference: Reference
    filing: Filing | None


@dataclass
class Company:
    enterprise_id: str
    filings: list[CompanyFiling]
    persons: dict[tuple[str, str], dict] = field(
        default_factory=dict, repr=False, init=False
    )
    entities: dict[str, dict] = field(
        default_factory=dict, repr=False, init=False
    )

    @property
    def latest(self) -> CompanyFiling:
        return self.filings[0]

    def resolve_persons(self, first_name: str, last_name: str) -> dict[str, Any]:
        new_fwd, new_rev = _person_search_key(first_name, last_name)
        for (fwd, _) in self.persons.keys():
            threshold = _fuzzy_threshold(fwd)
            if (
                fuzz.ratio(fwd, new_fwd) >= threshold
                or fuzz.ratio(fwd, new_rev) >= threshold
            ):
                return self.persons[(fwd, _)]
        
        new_uuid = str(uuid.uuid4())
        self.persons[(new_fwd, new_rev)] = {
            'person': {
                'person_uuid': new_uuid,
                'search_name': new_fwd,
                'search_name_reversed': new_rev
            },
            'admin_nat': [],
            'mandates': []
        }
        return self.persons[(new_fwd, new_rev)]

    def resolve_entities(self, identifier: str | None, name: str | None) -> dict:
        if identifier:
            if identifier not in self.entities:
                self.entities[identifier] = {
                    "entity": {
                        'entity_uuid': str(uuid.uuid4()),
                        'search_name': name.strip().lower() if name else identifier or None
                    },
                    "admin_legal": [],
                    "mandates": []
                }
            return self.entities[identifier]

        clean = (name or "").casefold().replace(" ", "")
        for key in self.entities.keys():
            if fuzz.ratio(key, clean) >= _fuzzy_threshold(key):
                return self.entities[key]
        new_uuid = uuid.uuid4()
        self.entities[clean] = {
            "entity": {
                'entity_uuid': new_uuid,
                'search_name': name.strip().lower() if name else identifier or None
            },
            "admin_legal": [],
            "mandates": []
        }
        return self.entities[clean]

    @classmethod
    def from_source(
        cls,
        enterprise_id: str,
        refs_json: list[dict],
        filings_by_ref: dict[str, dict],
    ) -> "Company":
        paired = []
        for ref_data in refs_json:
            ref = Reference.from_dict(ref_data)
            filing_data = filings_by_ref.get(ref.reference_number)
            filing = Filing.from_dict(filing_data, ref.account_year) if filing_data else None
            paired.append(CompanyFiling(reference=ref, filing=filing))

        paired.sort(key=lambda cf: cf.reference.exercise_dates.end_date, reverse=True)
        return cls(enterprise_id=enterprise_id, filings=paired)
