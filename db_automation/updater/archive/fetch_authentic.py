from datetime import datetime

import requests
from sqlalchemy.dialects.postgresql import insert

import db_automation.database.models_archive as mdl_a
from db_automation.api.classes import QueryNbbConsult
from db_automation.database.classes import DbConnector
from db_automation.updater.archive.functions import upper_first_char_keys
from db_automation.updater.archive.functions import sort_keep_lastest


def get_URL(obj):
    lst = []

    if isinstance(obj, dict):
        x = obj.get('ReferenceNumber')
        if x:
            lst = [x]
        return lst

    elif isinstance(obj, list):
        lst = [
            d['ReferenceNumber']
            for d in obj
            if d.get('ReferenceNumber')
        ]
        return lst
    return lst


def first_entry(e_id):
    """Insert references and filings for the first time."""

    db = DbConnector(db='cd_nbb_archive')

    i = 1
    attempt = True
    while attempt:
        try:
            r = QueryNbbConsult(
                {
                    "db": "authentic",
                    "request": "ref",
                    "ref_id": e_id
                }
            ).response.json()
            attempt = False
        except requests.exceptions.ConnectionError:
            i += 1
            if i > 3:
                attempt = False

    references = upper_first_char_keys(r)
    if isinstance(references, dict):
        references = [references]
    references = sort_keep_lastest(references)

    # First phase: insert known references.
    stmt = (
        insert(mdl_a.Reference)
        .values({
            "enterprise_id": e_id,
            "json_reference": references,
            "last_update": datetime.now()
        })
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["enterprise_id"],
        set_={
            "json_reference": stmt.excluded.json_reference,
            "last_update": stmt.excluded.last_update
        }
    )

    with db.engine.begin() as conn:
        conn.execute(stmt)

    # Second phase: inserting known statements.
    filing_ids = get_URL(references)
    filing_statements = []

    for f_id in filing_ids:
        res = QueryNbbConsult(
            {
                "db": "authentic",
                "request": "accData",
                "ref_id": f_id
            }
        ).response

        if res.status_code != 200:
            continue

        r = res.json()

        stmt = (
            insert(mdl_a.Filing)
            .values({
                "enterprise_id": e_id,
                "filing_id": f_id,
                "json_filing": r,
                "last_update": datetime.now()
            })
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=['enterprise_id', 'filing_id'],
            set_={
                "json_filing": stmt.excluded.json_filing,
                "last_update": stmt.excluded.last_update
            }
        )
        filing_statements.append(stmt)

    with db.engine.begin() as conn:
        for stmt in filing_statements:
            conn.execute(stmt)
