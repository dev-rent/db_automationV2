import os
import sys
import zipfile
from datetime import date, timedelta
from pathlib import Path

from db_automation.logger.config import update_nbb_logger
import db_automation.updater.archive.functions as fnc


yesterday = str(date.today() - timedelta(days=1))

temp_folder = Path(os.path.join(
        os.getcwd(), "db_automation/updater/archive/tmp"
))
temp_folder.mkdir(parents=True, exist_ok=True)

folder_ref = Path(os.path.join(temp_folder, "references"))
zip_ref = Path(os.path.join(temp_folder, "references.zip"))

folder_filing = Path(os.path.join(temp_folder, "filings"))
zip_filing = Path(os.path.join(temp_folder, "filings.zip"))


def fetch_extracts():
    """Fetch ZIP and extract files in temporary folder."""

    update_nbb_logger.info('Start fetch & extract process')

    success = fnc.extracts_call(
        dest_zip=str(zip_ref),
        date=yesterday,
        references=True
    )

    if not success:
        update_nbb_logger.error(
            'Retrieving ZIP references was unsuccessful. '
            'Shutting down process...'
        )
        sys.exit()

    success = fnc.extracts_call(
        dest_zip=str(zip_filing),
        date=yesterday,
        references=False
    )

    if not success:
        update_nbb_logger.error(
            'Retrieving ZIP filings was unsuccessful. '
            'Shutting down process...'
        )
        sys.exit()

    with zipfile.ZipFile(zip_ref, "r") as z:
        z.extractall(folder_ref)

    with zipfile.ZipFile(zip_filing, "r") as z:
        z.extractall(folder_filing)

    update_nbb_logger.info('Ended fetch & extract process')
