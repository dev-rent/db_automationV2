from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import select
from sqlalchemy.engine import Engine

import db_automation.database.models_archive as mdl_a
import db_automation.database.models_nbb as nbb
import db_automation.updater.nbb.functions as fnc
from db_automation.database.classes import DbConnector
from db_automation.logger.config import update_nbb_logger


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
            conn_from
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


def _process_one(e_id: str, engine_from: Engine, engine_to: Engine) -> None:
    with engine_from.connect() as conn_from, engine_to.connect() as conn_to:
        company = fnc.extractor(e_id, conn_from)
        if company is None:
            update_nbb_logger.error(f'References not present for {e_id}')
            return
        fnc.loader(fnc.transformer(company), conn_to)
        update_nbb_logger.info(f"Completed: {e_id}")


def populate_destination(
    db_from: str,
    db_to: str,
    to_update: set[str],
    max_workers: int = 6,
):
    """ETL raw data in CompanyData database."""

    source_db = DbConnector(db=db_from, pool_size=max_workers)
    dest_db = DbConnector(db=db_to, pool_size=max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_process_one, e_id, source_db.engine, dest_db.engine): e_id
            for e_id in to_update
        }
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                update_nbb_logger.exception(f"Failed for {futures[future]}")














#         result = (
#             conn_from.execution_options(stream_results=True)
#             .execute(stmt)
#             .mappings()
#         )

#         for row in result:
#             enterprise_id: str = row["enterprise_id"]
#             references: list[Reference] | None = (
#                 [Reference.from_dict(item) for item in row['json_reference']] 
#                 if row.get("json_reference") 
#                 else None
#             )

#             if not references:
#                 update_nbb_logger.info(
#                     f"Skipped {enterprise_id} because no reference or reference is empty"
#                 )
#                 continue

#             latest_references = fnc.sort_keep_lastest(references)

#             if not latest_references:
#                 update_nbb_logger.info("Skipped %s — all references before 2021", enterprise_id)
#                 continue

#             latest = latest_references[0]
#             update_nbb_logger.info("")
#             update_nbb_logger.info(
#                 "------------Trying new enterprise: %s ------------", enterprise_id
#             )

#             ci_batch.append({
#                 "enterprise_id": latest.enterprise_number,
#                 "denomination": latest.enterprise_name,
#                 "legal_situation": latest.legal_situation,
#                 "search_field": (
#                     ''.join(latest.enterprise_name.split())
#                     if latest.enterprise_name else None
#                 ),
#                 "last_update": datetime.now()
#             })

#             for ref in latest_references:
#                 statement_batch.append({
#                     "filing_id": ref.reference_number,
#                     "enterprise_id": ref.enterprise_number,
#                     "start_date": ref.exercise_dates.start_date,
#                     "end_date": ref.exercise_dates.end_date,
#                     "account_year": ref.account_year,
#                     "deposit_date": ref.deposit_date,
#                     "deposit_type": ref.deposit_type,
#                     "currency": ref.currency,
#                     "legal_form": ref.legal_form,
#                     "activity_code": ref.activity_code,
#                     "model_type": ref.model_type,
#                     "account_url": bool(ref.account_data_url),
#                     "legal_validation": ref.legal_validation,
#                     "assembly_date": ref.assembly_date,
#                     "data_version": ref.data_version,
#                     "improvement_date": ref.improvement_date,
#                     "corrected_data": ref.corrected_data,
#                     "last_update": datetime.now(),
#                 })

#             if len(statement_batch) >= batch_size:
#                 fnc.write_batch(
#                     engine=dest_db.engine,
#                     ci_batch=ci_batch,
#                     statement_batch=statement_batch
#                 )
#                 ci_batch.clear()
#                 statement_batch.clear()

#         if ci_batch:
#             fnc.write_batch(
#                 engine=dest_db.engine,
#                 ci_batch=ci_batch,
#                 statement_batch=statement_batch
#             )


# def populate_destination_filings(
#     db_from: str,
#     db_to: str,
#     to_update: set[str]
# ):
#     """Populate all filings in CompanyData database."""

#     source_db = DbConnector(db=db_from)
#     dest_db = DbConnector(db=db_to)

#     batch_size = 500
#     ci_batch = []
#     statement_batch = []

#     for e_id in to_update:



#     with source_db.engine.connect() as conn_from:
#         result = (
#             conn_from.execution_options(stream_results=True)
#             .execute(stmt)
#             .mappings()
#         )

#         for row in result:
#             enterprise_id: str = row["enterprise_id"]
#             references: list[Reference] | None = (
#                 [Reference.from_dict(item) for item in row['json_reference']] 
#                 if row.get("json_reference") 
#                 else None
#             )

#             if not references:
#                 update_nbb_logger.info(
#                     f"Skipped {enterprise_id} because no reference or reference is empty"
#                 )
#                 continue

#             latest_references = fnc.sort_keep_lastest(references)

#             if not latest_references:
#                 update_nbb_logger.info("Skipped %s — all references before 2021", enterprise_id)
#                 continue

#             latest = latest_references[0]
#             update_nbb_logger.info("")
#             update_nbb_logger.info(
#                 "------------Trying new enterprise: %s ------------", enterprise_id
#             )

#             ci_batch.append({
#                 "enterprise_id": latest.enterprise_number,
#                 "denomination": latest.enterprise_name,
#                 "legal_situation": latest.legal_situation,
#                 "search_field": (
#                     ''.join(latest.enterprise_name.split())
#                     if latest.enterprise_name else None
#                 ),
#                 "last_update": datetime.now()
#             })

#             for ref in references:
#                 statement_batch.append({
#                     "filing_id": ref.reference_number,
#                     "enterprise_id": ref.enterprise_number,
#                     "start_date": ref.exercise_dates.start_date,
#                     "end_date": ref.exercise_dates.end_date,
#                     "account_year": ref.account_year,
#                     "deposit_date": ref.deposit_date,
#                     "deposit_type": ref.deposit_type,
#                     "currency": ref.currency,
#                     "legal_form": ref.legal_form,
#                     "activity_code": ref.activity_code,
#                     "model_type": ref.model_type,
#                     "account_url": bool(ref.account_data_url),
#                     "legal_validation": ref.legal_validation,
#                     "assembly_date": ref.assembly_date,
#                     "data_version": ref.data_version,
#                     "improvement_date": ref.improvement_date,
#                     "corrected_data": ref.corrected_data,
#                     "last_update": datetime.now(),
#                 })

#             if len(statement_batch) >= batch_size:
#                 fnc.write_batch(
#                     engine=dest_db.engine,
#                     ci_batch=ci_batch,
#                     statement_batch=statement_batch
#                 )
#                 ci_batch.clear()
#                 statement_batch.clear()

#         if ci_batch:
#             fnc.write_batch(
#                 engine=dest_db.engine,
#                 ci_batch=ci_batch,
#                 statement_batch=statement_batch
#             )
