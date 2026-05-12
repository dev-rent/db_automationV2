from collections import defaultdict
from datetime import date, datetime
from sqlalchemy import select
from sqlalchemy.engine import Connection
from sqlalchemy.dialects.postgresql import insert

import db_automation.database.models_archive as mdl_a
import db_automation.database.models_nbb as nbb
from db_automation.updater.nbb.classes import Reference, Company


def sort_keep_lastest(ref_list: list) -> list:
    """Sort dictionaries and keep the ones after 2021."""

    lst = sorted(
        (
            x for x in ref_list
            if date.fromisoformat(x["ExerciseDates"]["EndDate"]).year >= 2021
        ),
        key=lambda x: (x["ExerciseDates"]["EndDate"], x["DepositDate"])
    )
    return lst


def extractor(enterprise_id: str, connection: Connection) -> Company | None:
    stmt = (
        select(mdl_a.Reference.json_reference, mdl_a.Filing.json_filing)
        .outerjoin(mdl_a.Filing, mdl_a.Reference.enterprise_id == mdl_a.Filing.enterprise_id)
        .where(mdl_a.Reference.enterprise_id == enterprise_id)
    )

    rows = connection.execute(stmt).mappings().all()

    if not rows:
        return None

    refs_json = rows[0]["json_reference"]
    if refs_json is None:
        return None

    latest_refs_json = sort_keep_lastest(refs_json)
    filings_by_ref = {
        row["json_filing"]["ReferenceNumber"]: row["json_filing"]
        for row in rows
        if row["json_filing"] is not None
    }
    return Company.from_source(enterprise_id, latest_refs_json, filings_by_ref)


def transformer(company: Company) -> Company:
    for company_filing in company.filings[::-1]:
        if company_filing.filing is None:
            continue
        for person in company_filing.filing.natural_persons:
            person_dict = company.resolve_persons(
                person.first_name, person.last_name
            )
            person_uuid = person_dict['person'].get('person_uuid')
            person_dict['person'].update(
                first_name=person.first_name,
                last_name=person.last_name,
                street=person.address.street,
                street_number=person.address.number,
                box=person.address.box,
                zipcode=person.address.city or person.address.other_postal_code,
                country_code=person.address.country,
                profession= person.profession
            )
            person_dict['admin_nat'].append(
                {
                    "enterprise_id": company.enterprise_id,
                    "person_uuid": person_uuid,
                    "filing_id": company_filing.filing.reference_number,
                    "account_year": company_filing.filing.account_year,
                }
            )
            
            for mandate in person.mandates:
                person_dict['mandates'].append(
                    {
                        "person_uuid": person_uuid,
                        "enterprise_id": company.enterprise_id,
                        "filing_id": company_filing.filing.reference_number,
                        "function_code": mandate.function_code,
                        "start_date": mandate.mandate_dates.start_date,
                        "end_date": mandate.mandate_dates.end_date,
                        "account_year": company_filing.filing.account_year,
                    }
                )

        for entity in company_filing.filing.legal_persons:
            entity_dict = company.resolve_entities(
                entity.entity.identifier, entity.entity.name
            )
            entity_uuid = entity_dict['entity'].get('entity_uuid')
            entity_dict['entity'].update(
                entity_id=entity.entity.identifier or entity_uuid,
                country_code=entity.entity.address.country or "",
                denomination=entity.entity.name,
                street=entity.entity.address.street,
                street_number=entity.entity.address.number,
                box=entity.entity.address.box,
                zipcode=entity.entity.address.city or entity.entity.address.other_postal_code,
                search_name=(
                    ''.join(entity.entity.name.lower().strip())
                    if entity.entity.name else None
                )
            )

            for rep in entity.representatives:
                rep_dict = company.resolve_persons(
                    rep.first_name, rep.last_name
                )
                rep_uuid = rep_dict['person'].get('person_uuid')
                entity_dict['admin_legal'].append(
                    {
                        "enterprise_id": company.enterprise_id,
                        "person_uuid": rep_uuid,
                        "entity_uuid": entity_uuid,
                        "filing_id": company_filing.filing.reference_number,
                        "account_year": company_filing.filing.account_year,
                    }
                )
                rep_dict['person'].update(
                    first_name=rep.first_name,
                    last_name=rep.last_name,
                    street=rep.address.street,
                    street_number=rep.address.number,
                    box=rep.address.box,
                    zipcode=rep.address.city or rep.address.other_postal_code,
                    country_code=rep.address.country,
                    profession= rep.profession
                )
                
                for mandate in entity.mandates:
                    entity_dict['mandates'].append(
                        {
                            "person_uuid": rep_uuid,
                            "enterprise_id": company.enterprise_id,
                            "filing_id": company_filing.filing.reference_number,
                            "function_code": mandate.function_code,
                            "start_date": mandate.mandate_dates.start_date,
                            "end_date": mandate.mandate_dates.end_date,
                            "account_year": company_filing.filing.account_year,
                        }
                    )
        for shareholder in company_filing.filing.shareholders_individual:
            sh_dict = company.resolve_persons(shareholder.first_name, shareholder.last_name)
            sh_uuid = sh_dict['person'].get('person_uuid')
            sh_dict['person'].update(
                first_name=shareholder.first_name,
                last_name=shareholder.last_name,
                street=shareholder.address.street,
                street_number=shareholder.address.number,
                box=shareholder.address.box,
                zipcode=shareholder.address.city or shareholder.address.other_postal_code,
                country_code=shareholder.address.country,
                profession=None,
            )
            sh_dict.setdefault('shareholders_person', [])
            for rights in shareholder.rights_held:
                sh_dict['shareholders_person'].append(
                    {
                        "enterprise_id": company.enterprise_id,
                        "person_uuid": sh_uuid,
                        "filing_id": company_filing.filing.reference_number,
                        "account_year": company_filing.filing.account_year,
                        "nature_rights": rights.nature,
                        "line_rights": rights.line,
                        "securities_attached": int(float(rights.number_securities_attached)) if rights.number_securities_attached is not None else None,
                        "not_securities_attached": rights.number_not_securities_attached,
                        "percentage": float(rights.percentage) if rights.percentage is not None else None,
                    }
                )

        for shareholder in company_filing.filing.shareholders_entity:
            sh_dict = company.resolve_entities(shareholder.entity.identifier, shareholder.entity.name)
            sh_uuid = sh_dict['entity'].get('entity_uuid')
            sh_dict['entity'].update(
                entity_id=shareholder.entity.identifier or sh_uuid,
                country_code=shareholder.entity.address.country or "",
                denomination=shareholder.entity.name,
                street=shareholder.entity.address.street,
                street_number=shareholder.entity.address.number,
                box=shareholder.entity.address.box,
                zipcode=shareholder.entity.address.city or shareholder.entity.address.other_postal_code,
            )
            sh_dict.setdefault('shareholders_entity', [])
            for rights in shareholder.rights_held:
                sh_dict['shareholders_entity'].append(
                    {
                        "enterprise_id": company.enterprise_id,
                        "entity_uuid": sh_uuid,
                        "filing_id": company_filing.filing.reference_number,
                        "account_year": company_filing.filing.account_year,
                        "nature_rights": rights.nature,
                        "line_rights": rights.line,
                        "securities_attached": int(float(rights.number_securities_attached)) if rights.number_securities_attached is not None else None,
                        "not_securities_attached": rights.number_not_securities_attached,
                        "percentage": float(rights.percentage) if rights.percentage is not None else None,
                    }
                )

        for partint in company_filing.filing.participating_interests:
            partint_dict = company.resolve_entities(
                partint.entity.identifier, partint.entity.name
            )
            entity_uuid = partint_dict['entity'].get('entity_uuid')
            partint_dict['entity'].update(
                entity_id=partint.entity.identifier or entity_uuid,
                country_code=partint.entity.address.country or "",
                denomination=partint.entity.name,
                street=partint.entity.address.street,
                street_number=partint.entity.address.number,
                box=partint.entity.address.box,
                zipcode=partint.entity.address.city or partint.entity.address.other_postal_code,
            )
            partint_dict.setdefault('participating_interests', [])
            for interest in partint.interests_held:
                partint_dict['participating_interests'].append(
                    {
                        "enterprise_id": company.enterprise_id,
                        "entity_uuid": entity_uuid,
                        "filing_id": company_filing.filing.reference_number,
                        "account_year": company_filing.filing.account_year,
                        "account_date": partint.account_date,
                        "currency": partint.currency or "",
                        "equity": float(partint.equity) if partint.equity is not None else None,
                        "net_result": float(partint.net_result) if partint.net_result is not None else None,
                        "nature": interest.nature,
                        "line": interest.line,
                        "amount": int(float(interest.number)) if interest.number is not None else None,
                        "percentage_held": float(interest.percentage_directly_held) if interest.percentage_directly_held is not None else None,
                        "percentage_subsidiary": float(interest.percentage_subsidiaries) if interest.percentage_subsidiaries is not None else None,
                    }
                )

    return company


def loader(company: Company, connection: Connection):

    UPDATE_TS = datetime.now()

    # Stateless statements
    stmt_ci = insert(nbb.CompanyInfo)
    stmt_statements = insert(nbb.Statement)
    stmt_codes = insert(nbb.AccountingCode)
    stmt_facts = insert(nbb.StatementFact)
    stmt_persons = insert(nbb.NaturalPerson)
    stmt_entities = insert(nbb.Entity)
    stmt_admin_n = insert(nbb.AdministratorNatural)
    stmt_admin_l = insert(nbb.AdministratorLegal)
    stmt_mand = insert(nbb.Mandate)
    stmt_partint = insert(nbb.ParticipatingInterest)
    stmt_sh_person = insert(nbb.PersonShareholder)
    stmt_sh_entity = insert(nbb.EntityShareholder)


    rows_ci = [
        {
            "enterprise_id": company.enterprise_id,
            "denomination": company.latest.reference.enterprise_name,
            "legal_situation": company.latest.reference.legal_situation,
            "search_field": (
                ''.join(company.latest.reference.enterprise_name.lower().split())
                if company.latest.reference.enterprise_name else None
            ),
            "last_update": UPDATE_TS
        }
    ]
    rows_statements = []
    rows_facts = []

    for filing in company.filings:
        rows_statements.append(
            {
                "filing_id": filing.reference.reference_number,
                "enterprise_id": company.enterprise_id,
                "start_date": filing.reference.exercise_dates.start_date,
                "end_date": filing.reference.exercise_dates.end_date,
                "account_year": filing.reference.account_year,
                "deposit_date": filing.reference.deposit_date,
                "deposit_type": filing.reference.deposit_type,
                "currency": filing.reference.currency,
                "legal_form": filing.reference.legal_form,
                "activity_code": filing.reference.activity_code,
                "model_type": filing.reference.model_type,
                "account_url": bool(filing.reference.account_data_url),
                "legal_validation": filing.reference.legal_validation,
                "assembly_date": filing.reference.assembly_date,
                "data_version": filing.reference.data_version,
                "improvement_date": filing.reference.improvement_date,
                "corrected_data": filing.reference.corrected_data,
                "last_update": UPDATE_TS
            }
        )
        if filing.filing is not None:
            r_merged = defaultdict(lambda: {"book_value": None, "previous_value": None})
            for rubric in filing.filing.rubrics:
                if rubric.period == "N":
                    r_merged[rubric.code]["book_value"] = float(rubric.value)
                elif rubric.period == "NM1":
                    r_merged[rubric.code]["previous_value"] = float(rubric.value)

            for code, values in r_merged.items():
                rows_facts.append({
                    "account_year": filing.reference.account_year,
                    "filing_id": filing.reference.reference_number,
                    "accountcode_id": code,
                    "book_value": values["book_value"],
                    "previous_value": values["previous_value"],
                })

    rows_person = []
    rows_admin_n = []
    rows_mand = []
    rows_sh_person = []
    for p in company.persons.values():
        rows_person.append(p['person'])
        rows_admin_n.extend(p['admin_nat'])
        rows_mand.extend(p.get('mandates', []))
        rows_sh_person.extend(p.get('shareholders_person', []))

    rows_entity = []
    rows_admin_l = []
    rows_partint = []
    rows_sh_entity = []
    for e in company.entities.values():
        rows_entity.append(e['entity'])
        rows_admin_l.extend(e.get('admin_legal', []))
        rows_mand.extend(e.get('mandates', []))
        rows_partint.extend(e.get('participating_interests', []))
        rows_sh_entity.extend(e.get('shareholders_entity', []))
    
    upsert_codes = stmt_codes.on_conflict_do_nothing()
    upsert_facts = stmt_facts.on_conflict_do_nothing()
    upsert_ci = stmt_ci.on_conflict_do_update(
        index_elements=['enterprise_id'],
        set_={col: stmt_ci.excluded[col] for col in
            ['denomination', 'legal_situation', 'search_field', 'last_update']}
    )
    upsert_statements = stmt_statements.on_conflict_do_update(
        index_elements=['filing_id'],
        set_={col: stmt_statements.excluded[col] for col in
            ['enterprise_id', 'start_date', 'end_date', 'account_year',
             'deposit_date', 'deposit_type', 'currency', 'legal_form',
             'activity_code', 'model_type', 'account_url', 'legal_validation',
             'assembly_date', 'data_version', 'improvement_date',
             'corrected_data', 'last_update']}
    )
    upsert_persons = stmt_persons.on_conflict_do_update(
        constraint='uniq_person',
        set_={col: stmt_persons.excluded[col] for col in
            ['person_uuid', 'street_number', 'box', 'country_code',
             'search_name', 'search_name_reversed']}
    )
    upsert_entities = stmt_entities.on_conflict_do_update(
        index_elements=["entity_id", "country_code"],
        set_={col: stmt_entities.excluded[col] for col in
            ['entity_uuid', 'country_code', 'denomination', 'street',
             'street_number', 'box', 'zipcode']}
    )
    upsert_admin_n = stmt_admin_n.on_conflict_do_nothing()
    upsert_admin_l = stmt_admin_l.on_conflict_do_nothing()
    upsert_mand = stmt_mand.on_conflict_do_update(
        constraint='uniq_mandate',
        set_={col: stmt_mand.excluded[col] for col in
            ['start_date', 'end_date']}
    )
    upsert_partint = stmt_partint.on_conflict_do_update(
        index_elements=['enterprise_id', 'entity_uuid', 'filing_id', 'account_year', 'currency', 'nature', 'line'],
        set_={col: stmt_partint.excluded[col] for col in
            ['account_date', 'equity', 'net_result', 'amount', 'percentage_held', 'percentage_subsidiary']}
    )
    upsert_sh_person = stmt_sh_person.on_conflict_do_update(
        constraint='uniq_sha_person',
        set_={col: stmt_sh_person.excluded[col] for col in
            ['securities_attached', 'not_securities_attached', 'percentage']}
    )
    upsert_sh_entity = stmt_sh_entity.on_conflict_do_update(
        constraint='uniq_sha_ent',
        set_={col: stmt_sh_entity.excluded[col] for col in
            ['securities_attached', 'not_securities_attached', 'percentage']}
    )

    connection.execute(upsert_ci, rows_ci)
    if rows_statements:
        connection.execute(upsert_statements, rows_statements)

    if rows_facts:
        codes = sorted(
            {row["accountcode_id"]: {"accountcode_id": row["accountcode_id"], "denomination": row["accountcode_id"]}
             for row in rows_facts}.values(),
            key=lambda x: x["accountcode_id"]
        )
        connection.execute(upsert_codes, codes)
        connection.execute(upsert_facts, rows_facts)

    if rows_person:
        connection.execute(upsert_persons, rows_person)

    if rows_entity:
        connection.execute(upsert_entities, rows_entity)

    if rows_admin_n:
        connection.execute(upsert_admin_n, rows_admin_n)

    if rows_admin_l:
        connection.execute(upsert_admin_l, rows_admin_l)

    if rows_mand:
        connection.execute(upsert_mand, rows_mand)

    if rows_partint:
        connection.execute(upsert_partint, rows_partint)

    if rows_sh_person:
        connection.execute(upsert_sh_person, rows_sh_person)

    if rows_sh_entity:
        connection.execute(upsert_sh_entity, rows_sh_entity)

    connection.commit()
