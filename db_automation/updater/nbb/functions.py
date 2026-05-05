from datetime import datetime

from sqlalchemy.dialects.postgresql import insert

import db_automation.database.models_nbb as nbb
from db_automation.updater.nbb.classes import Reference


def sort_keep_lastest(ref_list: list[Reference]) -> list[Reference]:
    """Sort references and keep the ones from 2021 onwards."""

    return sorted(
        (x for x in ref_list if x.exercise_dates.end_date.year >= 2021),
        key=lambda x: (x.exercise_dates.end_date, x.deposit_date),
        reverse=True,
    )


def write_batch(engine, ci_batch, statement_batch):
    stmt_ci = insert(nbb.CompanyInfo).values(ci_batch)
    stmt_ci = stmt_ci.on_conflict_do_update(
        index_elements=["enterprise_id"],
        set_={
            "denomination": stmt_ci.excluded.denomination,
            "legal_situation": stmt_ci.excluded.legal_situation,
            "search_field": stmt_ci.excluded.search_field,
            "last_update": stmt_ci.excluded.last_update
        },
    )

    stmt_st = insert(nbb.Statement).values(statement_batch)
    stmt_st = stmt_st.on_conflict_do_update(
        index_elements=["filing_id"],
        set_={
            "enterprise_id": stmt_st.excluded.enterprise_id,
            "start_date": stmt_st.excluded.start_date,
            "end_date": stmt_st.excluded.end_date,
            "account_year": stmt_st.excluded.account_year,
            "deposit_date": stmt_st.excluded.deposit_date,
            "deposit_type": stmt_st.excluded.deposit_type,
            "currency": stmt_st.excluded.currency,
            "legal_form": stmt_st.excluded.legal_form,
            "activity_code": stmt_st.excluded.activity_code,
            "model_type": stmt_st.excluded.model_type,
            "account_url": stmt_st.excluded.account_url,
            "legal_validation": stmt_st.excluded.legal_validation,
            "assembly_date": stmt_st.excluded.assembly_date,
            "data_version": stmt_st.excluded.data_version,
            "improvement_date": stmt_st.excluded.improvement_date,
            "corrected_data": stmt_st.excluded.corrected_data,
            "last_update": stmt_st.excluded.last_update
        },
    )

    with engine.begin() as conn:
        conn.execute(stmt_ci)
        conn.execute(stmt_st)
