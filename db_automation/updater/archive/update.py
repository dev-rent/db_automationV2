import os
import json
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from db_automation.updater.archive.fetch_authentic import first_entry
import db_automation.updater.archive.functions as fnc
import db_automation.database.models_archive as mdl
from db_automation.updater.archive.fetch_extracts import folder_ref
from db_automation.updater.archive.fetch_extracts import folder_filing
from db_automation.database.classes import DbConnector
from db_automation.logger.config import update_nbb_logger


def update():
    """"""

    # Correct dictionary keys of new data inplace
    # fnc.standarise_keys(str(folder_ref))

    # Map enterprises that will be updated
    tuple_set = set()

    with os.scandir(folder_ref) as it:
        for entry in it:
            if entry.is_file() and entry.name.endswith(".json"):
                with open(entry.path, 'r') as data:
                    file = json.load(data)

                e_id = file.get("EnterpriseNumber")
                f_id = file.get("ReferenceNumber")

                if e_id and f_id:
                    tuple_set.add((e_id, f_id, entry.path))
                else:
                    update_nbb_logger.error(
                        "Keys not found. %s: %s", e_id, f_id
                    )
                    continue

    # Update one at a time.
    db = DbConnector(db="cd_nbb_archive")

    while tuple_set:
        temp_tuple = tuple_set.pop()

        # Corresponding filing
        filing = os.path.join(
            str(folder_filing),
            f"{temp_tuple[1]}.json"
        )

        # Get existing record for enterprise from database
        stmt = (
            select(mdl.Reference.json_reference)
            .where(mdl.Reference.enterprise_id == temp_tuple[0])
        )

        with db.engine.begin() as conn:
            old_value = conn.scalar(stmt)

        if not old_value:  # First entry in database
            try:
                first_entry(temp_tuple[0])
                os.remove(temp_tuple[2])
                os.remove(filing)
                continue
            except Exception as e:
                update_nbb_logger.error(
                    'Failed inserting data %s for the first time.',
                    temp_tuple[0]
                )
                update_nbb_logger.exception(e)
                continue

        else:  # Get all known references and compare
            known_refs = set()
            for d in old_value:
                known_refs.add(d.get("ReferenceNumber"))

            if temp_tuple[1] in known_refs:
                continue
            else:
                # open file
                with open(temp_tuple[2], 'r') as data:
                    f = json.load(data)

                old_value.append(f)
                new_value = sorted(
                    old_value,
                    key=lambda x: x['ReferenceNumber'],
                    reverse=True
                )

        try:
            with open(filing, 'r') as f:
                new_filing = json.load(f)
        except Exception as e:
            update_nbb_logger.error("File not found: %s", temp_tuple[1])
            update_nbb_logger.exception(e)
            try:
                # fnc.insert_references(temp_tuple[0], new_value, db)
                update_nbb_logger.info("But references stored in DB.")
            except Exception:
                update_nbb_logger.error("Failed storing references.")
            continue

        # Prepare new statements
        stmt_ref = insert(mdl.Reference).values(
            {
                "enterprise_id": temp_tuple[0],
                "json_reference": new_value,
                "last_update": datetime.now()
            }
        )
        stmt_ref = stmt_ref.on_conflict_do_update(
            index_elements=["enterprise_id"],
            set_={
                "json_reference": stmt_ref.excluded.json_reference,
                "last_update": stmt_ref.excluded.last_update
            }
        )

        stmt_filing = insert(mdl.Filing).values(
            {
                "enterprise_id": temp_tuple[0],
                "filing_id": temp_tuple[1],
                "json_filing": new_filing,
                "last_update": datetime.now()
            }
        )
        stmt_filing = stmt_filing.on_conflict_do_update(
            index_elements=["enterprise_id", "filing_id"],
            set_={
                "json_filing": stmt_filing.excluded.json_filing,
                "last_update": stmt_filing.excluded.last_update
            }
        )

        with db.engine.begin() as conn:
            conn.execute(stmt_ref)
            conn.execute(stmt_filing)

        os.remove(temp_tuple[2])
        os.remove(filing)
