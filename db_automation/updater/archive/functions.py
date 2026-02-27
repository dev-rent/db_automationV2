import time

from db_automation.api.classes import QueryNbbConsult
from db_automation.logger.config import update_nbb_logger


def api_call(
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
