import os
import zipfile
from datetime import datetime, date

import requests

from db_automation.config import Config
from db_automation.logger.config import update_cbe_logger


class ZipBot:
    """A bot that fetches daily updates from the CBE.

    Parameters
    ----------
    date : datetime
        date of ZIP file (default yesterday).

    Attributes
    ----------
    date : datetime
        the user-given date or default.
    response : requests.Response
        Response object.
    success : bool
        whether operation succeeded or not.

    Methods
    -------
    save_to(dest=Config.ZIP_DESTINATION)
        function takes destination (dest: str) as argument.
    open_zip(file=(Config.ZIP_DESTINATION + Config.ZIP_FILENAME))
        function that opens ZIP and store content on destined location.
    """

    def __init__(self, date_: date):
        self.date = date_
        self.success: bool = False
        self.ref_id = self._calculate_zip_id()
        self.response = self._make_request()

    def _calculate_zip_id(self):
        """Return calculated zip_id.

        Current format for ZIP file is
        "KboOpenData_XXXX_Year_Month_day_Update.zip" where XXXX is an ascending
        number. The zero point is unclear so, as of 05 october 2025, it's set
        on 140. The next day will thus have number 141.
        """

        update_cbe_logger.info("Calculating ZIP ID.")

        zero_date = datetime.strptime(Config.ZERO_DATE, "%Y-%m-%d").date()
        zero_point = int(Config.ZERO_POINT)
        diff = self.date - zero_date
        zip_id = f"0{zero_point + diff.days}"

        update_cbe_logger.info(f"ZIP ID is set to {zip_id}")
        return zip_id

    def _make_request(self):
        """Return response."""

        session = requests.Session()
        session.headers.update({'User-agent': Config.USER_AGENT})

        response = session.post(
            Config.URL_LOGIN,
            data={
                'j_username': Config.CBE_USER,
                'j_password': Config.CBE_PASSWORD
            },
            allow_redirects=True
        )

        if (
            response.status_code != 200
            or response.history
            and response.history[0].status_code != 302
        ):
            update_cbe_logger.error((
                f"Login failed. Status code: {response.status_code}, "
                f"message: {response.reason}"
                ))
            return None
        else:
            update_cbe_logger.info("Login successful.")

            file_url = Config.URL_ZIP.format(
                self._calculate_zip_id(),
                self.date.strftime("%Y_%m_%d")
                )

            update_cbe_logger.info("Trying URL: %s", file_url)

            zip_response = session.get(file_url)

            if zip_response.status_code != 200:
                update_cbe_logger.error((
                    "Failed to download ZIP file. "
                    f"Status code: {zip_response.status_code}, "
                    f"message: {response.reason}"
                    ))
                return zip_response
            else:
                update_cbe_logger.info("ZIP file downloaded successfully.")
                self.success = True
                return zip_response

    def save_to(self, dest, filename):
        """Save response to filepath."""

        file = os.path.join(dest, filename)

        if not self.response or self.response.status_code != 200:
            update_cbe_logger.exception(
                AttributeError("Response object has no usable content.")
            )
            return None

        if not os.path.exists(dest):
            update_cbe_logger.error("Creating directory...")
            os.makedirs(dest)

        update_cbe_logger.info("Writing response content to ZIP file...")
        with open(file, "wb") as f:
            f.write(self.response.content)

    def open_zip(self, dest, filename):
        """Open the zipfile."""

        update_cbe_logger.info("Starting saving process...")
        filepath = os.path.join(dest, filename)

        with zipfile.ZipFile(filepath, mode='r',) as reader:
            reader.extractall(path=dest)
        self.success = True
