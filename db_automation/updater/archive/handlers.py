import os
import json
import sys
import time
import zipfile
from datetime import datetime

from sqlalchemy.dialects.postgresql import insert

import db_automation.updater.archive.functions as fnc
from db_automation.api.classes import QueryNbbConsult
from db_automation.config import Config
from db_automation.database.classes import DbConnector
import db_automation.database.models_archive as mdl
from db_automation.logger.config import update_nbb_logger
from db_automation.mailer.send_mail import send_update_mail


def fetch_extracts(
    dest_zip: str,
    dest_folder: str,
    date_: str,
    references: bool
):
    """Fetch ZIP and extract files in temporary folder."""

    update_nbb_logger.info('Start fetch & extract process')

    if references:
        request = "ref"
    else:
        request = "accData"

    success = False
    i = 0

    while i < 3:
        try:
            call = QueryNbbConsult(
                {
                    "db": "extracts",
                    "request": request,
                    "date": date_
                }
            )
            response = call.response.content

            with open(dest_zip, "wb") as f:
                f.write(response)

            success = True
            break
        except Exception as e:
            i += 1
            update_nbb_logger.error(
                "Failed attempt %s, fetching %s: %s", i, request, e
            )

            time.sleep(3)  # Prevents error requests.exceptions.ConnectionError: HTTPSConnectionPool(host='ws.cbso.nbb.be', port=443): Max retries exceeded with url: /extracts/batch/2026-03-14/references (Caused by NameResolutionError("HTTPSConnection(host='ws.cbso.nbb.be', port=443): Failed to resolve 'ws.cbso.nbb.be' ([Errno 8] nodename nor servname provided, or not known)"))

            if i >= 3:
                success = False
                break

    if not success:
        update_nbb_logger.error(
            'Retrieving ZIP references was unsuccessful. '
            'Shutting down process...'
        )
        send_update_mail(success, Config.LOG_UPDATE_NBB)
        sys.exit()

    with zipfile.ZipFile(dest_zip, "r") as z:
        z.extractall(dest_folder)

    update_nbb_logger.info('Ended fetch & extract process')


def standarise_keys(path: str):
    """Use UpperCamelCase for all dictionary keys."""

    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file() and entry.name.endswith('.json'):
                with open(entry.path, 'r') as f:
                    data = json.load(f)

                new_data = fnc.upper_first_char_keys(data)

                with open(entry.path, 'w') as f:
                    f.write(json.dumps(new_data))


def update_database(db: str, dest_folder_ref: str, dest_folder_filing: str):
    """Update the given database."""

    failed = []
    update_set = fnc.map_folder(folder=dest_folder_ref, extension=".json")
    database = DbConnector(db=db)

    while update_set:
        temp_tuple = update_set.pop()
        e_id = temp_tuple[0]
        f_id = temp_tuple[1]

        reference = os.path.join(dest_folder_ref, f"{f_id}-reference.json")
        filing = os.path.join(str(dest_folder_filing), f"{f_id}.json")

        current_value_list = fnc.fetch_current_value(e_id, database)

        if not current_value_list:
            # First entry in database
            try:
                fnc.first_entry(e_id, database)
                fnc.delete_(reference)
                fnc.delete_(filing)
                continue
            except Exception as e:
                update_nbb_logger.exception(e)
                failed.append(e_id)
                continue

        else:
            # Get all known references and compare
            current_refs = set()
            for ref in current_value_list:
                current_refs.add(ref.get("ReferenceNumber"))

            if f_id in current_refs:
                # Filing data already in DB
                try:
                    fnc.delete_(reference)
                    fnc.delete_(filing)
                    continue
                except Exception as e:
                    update_nbb_logger.exception(e)
                    continue
            else:
                # New data, update references in database
                with open(reference, 'r') as data:
                    f = json.load(data)

                current_value_list.append(f)
                new_value_list = sorted(
                    current_value_list,
                    key=lambda x: x['ReferenceNumber'],
                    reverse=True
                )

        try:
            # Is file also provided by API (not a given)
            with open(filing, 'r') as f:
                new_filing = json.load(f)
        except Exception:
            try:
                call = QueryNbbConsult(
                    {
                        "db": "authentic",
                        "request": "accData",
                        "ref_id": f_id
                    }
                )
                if call.response.status_code != 200:
                    update_nbb_logger.error(
                        f"Filing {f_id} for {e_id} could not be found."
                    )
                    try:
                        fnc.delete_(reference)
                        fnc.delete_(filing)
                    except Exception as e:
                        update_nbb_logger.error(
                            f"No file to delete: {e}"
                        )
                    continue

                new_filing = call.response.json()
            except Exception as e:
                update_nbb_logger.error(f"Filing {f_id} not found: {e}")
                failed.append(e_id)
                continue

        # Prepare statements
        stmt_ref = insert(mdl.Reference).values(
            {
                "enterprise_id": e_id,
                "json_reference": new_value_list,
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
                "enterprise_id": e_id,
                "filing_id": f_id,
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

        with database.engine.begin() as conn:
            conn.execute(stmt_ref)
            conn.execute(stmt_filing)

        try:
            fnc.delete_(reference)
            fnc.delete_(filing)
        except Exception as e:
            update_nbb_logger.error(f"Failed deleting file: {e}")

    for fail in failed:
        update_nbb_logger.info("%s", fail)

    update_nbb_logger.info("Completed update process.")
