import os
import json
import time
from datetime import datetime

from db_automation.api.classes import QueryNbbConsult
from db_automation.logger.config import update_nbb_logger


def extracts_call(
    dest_zip: str,
    date: str,
    references=True
) -> bool:
    """Fetch new update files from NBB API. Return boolean for success.

    Parameters
    ----------
    references : bool
        Default `True` and fetches references. Else filings.
    dest_zip : str
        Destination for ZIP-file.
    """

    i = 1
    attempt = True
    success = False

    if references:
        request = "ref"
    else:
        request = "accData"

    # Fetch reference data from API
    while attempt:
        try:
            api = QueryNbbConsult(
                {
                    "db": "extracts",
                    "request": request,
                    "date": date
                }
            )
            time.sleep(2)  # Prevents error requests.exceptions.ConnectionError: HTTPSConnectionPool(host='ws.cbso.nbb.be', port=443): Max retries exceeded with url: /extracts/batch/2026-03-14/references (Caused by NameResolutionError("HTTPSConnection(host='ws.cbso.nbb.be', port=443): Failed to resolve 'ws.cbso.nbb.be' ([Errno 8] nodename nor servname provided, or not known)"))
            with open(dest_zip, "wb") as f:
                f.write(api.response.content)

            attempt = False
            success = True
        except Exception as e:
            update_nbb_logger.error(
                "Failed attempt %s, fetching %s", i, request
            )
            update_nbb_logger.exception(e)
            time.sleep(1)
            i += 1
            if i > 3:
                attempt = False
                success = False
    return success


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


def standarise_keys(path: str):
    """Use UpperCamelCase for all dictionary keys."""

    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file() and entry.name.endswith('.json'):
                with open(entry.path, 'r') as f:
                    data = json.load(f)

                new_data = upper_first_char_keys(data)

                with open(entry.path, 'w') as f:
                    f.write(json.dumps(new_data))


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
