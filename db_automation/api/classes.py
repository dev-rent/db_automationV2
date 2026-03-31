import re
import uuid

import requests

from db_automation.config import Config


class QueryNbbConsult:
    """A customised class to handle querying information from the NBB database.

    The main attribute is `self.response` and it represents the response object
    from the NBB API.

    This class has module `request` integrated and expects a dictionary with
    values based on the required combinations to generate a correct url and
    headers.

    Steps to determine the values needed:
    - choose `database`: `authentic` or `extracts`.
    - choose type of data for key `request`: `ref` or `accData`.
    - if `authentic`, provide `ref_id` with the correct ID (CBE or Filing).
    - if `extracts`, provide `date` as string format "%Y-%m-%d", with a date
    between -1 day and -30 days.

    Example
    -------
    ```
    params = {
        "db": "authentic",
        "request": "accData",
        "ref_id": "2025-XXXXXXX"
    }
    ```

    Attributes
    ----------
    response : request.Response
        Object containing the response.
    db : str
        The chosen database: `authentic` or `extracts`.
    request : str
        The chosen API endpoint: `ref` or `accData`.
    ref_id : str
        When `authentic`, provide CBE ID.
    date : str
        when 'extracts`, provide date as "%Y-%m-%d".
    """

    url_map = {
        "authentic": {
            "hdr": {
                "NBB-CBSO-Subscription-Key": Config.API_KEY_AUTH,
                "User-Agent": "PostmanRuntime/7.37.3"
            },
            "ref": {
                "url": "authentic/legalEntity/{}/references",
                "accept": "application/json"
            },
            "accData": {
                "url":  "authentic/deposit/{}/accountingData",
                "accept": "application/x.jsonxbrl",
                "pdf": "application/pdf"
            }
        },
        "extracts": {
            "hdr": {
                "NBB-CBSO-Subscription-Key": Config.API_KEY_EXTR,
                "User-Agent": "PostmanRuntime/7.37.3"
            },
            "ref": {
                "url": "extracts/batch/{}/references",
                "accept": "application/x.zip+json"
            },
            "accData": {
                "url": "extracts/batch/{}/accountingData",
                "accept": "application/x.zip+jsonxbrl"
            }
        }
    }

    def __init__(self, params: dict[str, str], *, pdf=False):
        self.db = params.get("db", "")
        self.request = params.get("request", "")
        self.ref_id = re.sub(r"[^\d\-]", "", params.get("ref_id", ""))
        self.date = params.get("date", "")
        self.pdf = pdf

    @property
    def url(self) -> str:
        endpoint = self.url_map[self.db][self.request]["url"]
        base_url = "https://ws.cbso.nbb.be/"
        url = base_url + endpoint

        if self.db == "authentic":
            if not self.ref_id:
                raise ValueError("'authentic' was chosen without CBE ID.")
            url = url.format(self.ref_id)

        elif self.db == "extracts":
            url = url.format(self.date)
        else:
            raise Exception("A correct URL could not be generated.")

        return url

    @property
    def header(self) -> dict[str, str]:
        header = self.url_map[self.db]["hdr"].copy()
        header["X-Request-Id"] = str(uuid.uuid4())

        if self.pdf:
            header['Accept'] = self.url_map[self.db][self.request]["pdf"]
        else:
            header['Accept'] = self.url_map[self.db][self.request]["accept"]

        return header

    @property
    def response(self) -> requests.Response:
        """Return API response."""

        return requests.get(url=self.url, headers=self.header)
