from datetime import datetime

from sqlalchemy import String, any_, bindparam, select
from sqlalchemy.dialects.postgresql import ARRAY, insert

import db_automation.database.models_archive as mdl_a
import db_automation.database.models_nbb as nbb
from db_automation.updater.nbb.classes import Reference
import db_automation.updater.nbb.functions as fnc
from db_automation.database.classes import DbConnector
from db_automation.logger.config import update_nbb_logger


def fetch_country_codes(db: str) -> dict[str, str]:
    """Fetch all ISO 3166 two-letter country codes keyed by Dutch name."""

    database = DbConnector(db=db)

    stmt = select(nbb.CountryCode.dutch_name, nbb.CountryCode.a_2)

    with database.engine.begin() as conn:
        result = conn.execute(stmt).mappings()
        country_codes = {
            row["dutch_name"].title(): row["a_2"].upper()
            for row in result
        }

    return country_codes


# TODO: For future, only check those with timestamp < 1 day/ 1 week /1 month
def prepare_update(db_from: str, db_to: str) -> set[str]:
    """Return enterprise IDs whose source timestamp is newer than the destination.

    Fetches all timestamps from both databases in two queries and compares them
    in memory. IDs absent from the destination are also included.
    """

    source_db = DbConnector(db=db_from)
    dest_db = DbConnector(db=db_to)

    stmt_source = (
        select(mdl_a.Reference.enterprise_id, mdl_a.Reference.last_update)
        .where(mdl_a.Reference.json_reference.is_not(None))
    )
    stmt_dest = select(nbb.CompanyInfo.enterprise_id, nbb.CompanyInfo.last_update)

    with (
        source_db.engine.connect() as conn_from,
        dest_db.engine.connect() as conn_to,
    ):
        source_rows = (
            conn_from.execution_options(stream_results=True)
            .execute(stmt_source)
            .mappings()
            .all()
        )
        dest_ts_by_id = {
            row["enterprise_id"]: row["last_update"]
            for row in conn_to.execute(stmt_dest).mappings()
        }

    stale_ids = {
        row["enterprise_id"]
        for row in source_rows
        if dest_ts_by_id.get(row["enterprise_id"]) is None
        or dest_ts_by_id[row["enterprise_id"]] < row["last_update"]
    }
    return stale_ids


def populate_destination_references(
    db_from: str,
    db_to: str,
    to_update: set[str]
):
    """Populate all references in CompanyData database."""

    source_db = DbConnector(db=db_from)
    dest_db = DbConnector(db=db_to)

    batch_size = 500
    ci_batch = []
    statement_batch = []

    stmt = (
        select(mdl_a.Reference.enterprise_id, mdl_a.Reference.json_reference)
        .where(
            mdl_a.Reference.enterprise_id == any_(
                bindparam("ids", value=list(to_update), type_=ARRAY(String))
            )
        )
    )

    with source_db.engine.connect() as conn_from:
        result = (
            conn_from.execution_options(stream_results=True)
            .execute(stmt)
            .mappings()
        )

        for row in result:
            enterprise_id: str = row["enterprise_id"]
            references: list[Reference] | None = (
                [Reference.from_dict(item) for item in row['json_reference']] 
                if row.get("json_reference") 
                else None
            )

            if not references:
                update_nbb_logger.info(
                    f"Skipped {enterprise_id} because no reference or reference is empty"
                )
                continue

            latest_references = fnc.sort_keep_lastest(references)

            if not latest_references:
                update_nbb_logger.info("Skipped %s — all references before 2021", enterprise_id)
                continue

            latest = latest_references[0]
            update_nbb_logger.info("")
            update_nbb_logger.info(
                "------------Trying new enterprise: %s ------------", enterprise_id
            )

            ci_batch.append({
                "enterprise_id": latest.enterprise_number,
                "denomination": latest.enterprise_name,
                "legal_situation": latest.legal_situation,
                "search_field": (
                    ''.join(latest.enterprise_name.split())
                    if latest.enterprise_name else None
                ),
                "last_update": datetime.now()
            })

            for ref in references:
                statement_batch.append({
                    "filing_id": ref.reference_number,
                    "enterprise_id": ref.enterprise_number,
                    "start_date": ref.exercise_dates.start_date,
                    "end_date": ref.exercise_dates.end_date,
                    "account_year": ref.account_year,
                    "deposit_date": ref.deposit_date,
                    "deposit_type": ref.deposit_type,
                    "currency": ref.currency,
                    "legal_form": ref.legal_form,
                    "activity_code": ref.activity_code,
                    "model_type": ref.model_type,
                    "account_url": bool(ref.account_data_url),
                    "legal_validation": ref.legal_validation,
                    "assembly_date": ref.assembly_date,
                    "data_version": ref.data_version,
                    "improvement_date": ref.improvement_date,
                    "corrected_data": ref.corrected_data,
                    "last_update": datetime.now(),
                })

            if len(statement_batch) >= batch_size:
                fnc.write_batch(
                    engine=dest_db.engine,
                    ci_batch=ci_batch,
                    statement_batch=statement_batch
                )
                ci_batch.clear()
                statement_batch.clear()

        if ci_batch:
            fnc.write_batch(
                engine=dest_db.engine,
                ci_batch=ci_batch,
                statement_batch=statement_batch
            )


# def populate_destination_references(db_from: str, db_to: str):
#     """Populate all references in CompanyData database."""

#     database_from = DbConnector(db=db_from)
#     database_to = DbConnector(db=db_to)

#     batch_size = 100
#     ci_batch = []
#     statement_batch = []

#     stmt = (
#         select(mdl_a.Reference.enterprise_id, mdl_a.Reference.json_reference)
#         .where(
#             mdl_a.Reference.json_reference.is_not(None)
#             # mdl_a.Reference.enterprise_id.in_(to_update)
#         )
#     )

#     with database_from.engine.connect() as conn_from:
#         result = (
#             conn_from.execution_options(stream_results=True)
#             .execute(stmt)
#             .mappings()
#         )

#         for row in result:
#             enterprise_id = row["enterprise_id"]
#             json_reference = row["json_reference"]

#             update_nbb_logger.info("")
#             update_nbb_logger.info(
#                 "------------Trying new enterprise: %s ------------", enterprise_id
#             )

#             if not json_reference:
#                 # TODO: write function to delete empty list for null-value
#                 update_nbb_logger.info(f"Skipped {enterprise_id} because empty")
#                 continue
#             latest = max(json_reference, key=lambda x: x["DepositDate"])

#             ci_batch.append({
#                 "enterprise_id": enterprise_id,
#                 "denomination": latest.get("EnterpriseName"),
#                 "legal_situation": latest.get("LegalSituation"),
#                 "last_update": datetime.now()
#             })

#             for ref in json_reference:
#                 statement_batch.append({
#                     "filing_id": ref.get("ReferenceNumber"),
#                     "enterprise_id": ref.get("EnterpriseNumber"),
#                     "start_date": fnc.to_datetime(ref["ExerciseDates"].get("StartDate")),
#                     "end_date": fnc.to_datetime(ref["ExerciseDates"].get("EndDate")),
#                     "account_year": fnc.to_datetime(ref.get("DepositDate"), year_only=True),
#                     "deposit_date": fnc.to_datetime(ref.get("DepositDate")),
#                     "deposit_type": ref.get("DepositType"),
#                     "currency": ref.get("Currency"),
#                     "legal_form": ref.get("LegalForm"),
#                     "activity_code": ref.get("ActivityCode"),
#                     "model_type": ref.get("ModelType"),
#                     "account_url": True if ref.get("AccountingDataURL") else False,
#                     "legal_validation": ref.get("FullFillLegalValidation"),
#                     "assembly_date": fnc.to_datetime(ref.get("GeneralAssemblyDate")),
#                     "data_version": ref.get("DataVersion"),
#                     "improvement_date": fnc.to_datetime(ref.get("ImprovementDate")),
#                     "corrected_data": ref.get("CorrectedData"),
#                     "last_update": datetime.now(),
#                 })

#             if len(ci_batch) >= batch_size:
#                 fnc.write_batch(
#                     engine=database_to.engine,
#                     ci_batch=ci_batch,
#                     statement_batch=statement_batch
#                 )
#                 ci_batch.clear()
#                 statement_batch.clear()

#         if ci_batch:
#             fnc.write_batch(
#                 engine=database_to.engine,
#                 ci_batch=ci_batch,
#                 statement_batch=statement_batch
#             )
