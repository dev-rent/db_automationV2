import os
import sys
from datetime import date, timedelta
from pathlib import Path

from db_automation.config import Config
from db_automation.logger.config import update_nbb_logger
from db_automation.updater.archive.handlers import (
    fetch_extracts, standarise_keys, update_database
)
from db_automation.mailer.send_mail import send_update_mail


temp_folder = Path(os.path.join(os.getcwd(), "db_automation/updater/archive/tmp"))
temp_folder.mkdir(parents=True, exist_ok=True)

dest_folder_ref = Path(os.path.join(temp_folder, "references"))
dest_zip_ref = Path(os.path.join(temp_folder, "references.zip"))

dest_folder_filing = Path(os.path.join(temp_folder, "filings"))
dest_zip_filing = Path(os.path.join(temp_folder, "filings.zip"))


def main():
    update_nbb_logger.info('Starting process...')
    date_ = str(date.today() - timedelta(days=8))

    try:
        fetch_extracts(
            dest_zip=str(dest_zip_ref),
            dest_folder=str(dest_folder_ref),
            date_=date_,
            references=True
        )

        fetch_extracts(
            dest_zip=str(dest_zip_filing),
            dest_folder=str(dest_folder_filing),
            date_=date_,
            references=False
        )
        standarise_keys(str(dest_folder_ref))
        update_database(
            db=Config.DB_ARCHIVE,
            dest_folder_ref=str(dest_folder_ref),
            dest_folder_filing=str(dest_folder_filing)
        )
        success = True
        send_update_mail(success, Config.LOG_UPDATE_NBB)
    except Exception as e:
        success = False
        send_update_mail(success, Config.LOG_UPDATE_NBB)
        update_nbb_logger.error(
            f"The following error caused the script to stop: {e}"
        )
        sys.exit()


if __name__ == "__main__":
    main()
