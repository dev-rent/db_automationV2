import os
import json
import time

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
