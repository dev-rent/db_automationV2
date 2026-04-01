import os
import json
import time
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

import db_automation.database.models_archive as mdl
from db_automation.api.classes import QueryNbbConsult
from db_automation.database.classes import DbConnector
from db_automation.logger.config import update_nbb_logger


def get_URL(obj) -> list:
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


def upper_first_char_keys(lst: list):
    if isinstance(lst, dict):
        return {
            (
                k[:1].upper() + k[1:]
                if isinstance(k, str) and k else k
            ): upper_first_char_keys(v)
            for k, v in lst.items()
        }
    elif isinstance(lst, list):
        return [upper_first_char_keys(item) for item in lst]
    else:
        return lst


def map_folder(folder: str, *, extension: str) -> set[tuple[str, str]]:
    """Return iterator of given folder."""

    tuple_set = set()

    with os.scandir(folder) as it:
        for entry in it:
            if entry.is_file() and entry.name.endswith(extension):
                with open(entry.path, 'r') as data:
                    file = json.load(data)
                    e_id = file.get("EnterpriseNumber")
                    f_id = file.get("ReferenceNumber")

                    if e_id and f_id:
                        tuple_set.add((e_id, f_id))
                    else:
                        update_nbb_logger.error(
                            "Keys not found. %s: %s", e_id, f_id
                        )
                        continue

    return tuple_set


def fetch_current_value(e_id: str, db_engine: DbConnector) -> list | None:
    """Return current value in database."""

    stmt = (
        select(mdl.Reference.json_reference)
        .where(mdl.Reference.enterprise_id == e_id)
    )

    with db_engine.engine.begin() as conn:
        current_value = conn.scalar(stmt)
    return current_value


def first_entry(e_id: str, db_engine: DbConnector):
    """Request and insert data as if a first entry."""

    i = 0

    while i < 3:
        try:
            call = QueryNbbConsult(
                {
                    "db": "authentic",
                    "request": "ref",
                    "ref_id": e_id
                }
            )
            response = call.response.json()
            break
        except Exception as e:
            time.sleep(3)

            i += 1
            if i >= 3:
                raise Exception(f"Failed inserting first entry {e_id}: {e}")

    references = upper_first_char_keys(response)

    if isinstance(references, dict):
        references = [references]
    references = sort_keep_lastest(references)

    # First phase: insert known references.
    stmt = (
        insert(mdl.Reference)
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

    with db_engine.engine.begin() as conn:
        conn.execute(stmt)

    # Second phase: inserting known statements.
    filing_ids = get_URL(references)
    filings_to_commit = []

    for f_id in filing_ids:
        call = QueryNbbConsult(
            {
                "db": "authentic",
                "request": "accData",
                "ref_id": f_id
            }
        )
        response = call.response

        if response.status_code != 200:
            continue

        r = response.json()

        stmt = (
            insert(mdl.Filing)
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
        filings_to_commit.append(stmt)

    with db_engine.engine.begin() as conn:
        for stmt in filings_to_commit:
            conn.execute(stmt)
        # implied conn.commit()


def sort_keep_lastest(ref_list: list) -> list:
    """Sort dictionaries and keep the ones after 2021."""

    lst = sorted(
        (
            x for x in ref_list
            if datetime.strptime(
                x["ExerciseDates"]["EndDate"],
                "%Y-%m-%d"
                ).year >= 2021
        ),
        key=lambda x: (x["ExerciseDates"]["EndDate"], x["DepositDate"]),
        reverse=True
        )
    return lst


def delete_(path: str):
    """Delete file/folder at given path.

    Params
    ------
    path : str
        path to delete.
    """

    try:
        os.remove(path)
    except Exception as e:
        raise Exception(f"Failed deleting path: {e}")
