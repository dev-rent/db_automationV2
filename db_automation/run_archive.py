import sys

from db_automation.config import Config
from db_automation.logger.config import update_nbb_logger
from db_automation.updater.archive.fetch_extracts import fetch_extracts
from db_automation.updater.archive.update import update
from db_automation.mailer.send_mail import send_update_mail


def main():
    update_nbb_logger.info('Starting process...')

    try:
        fetch_extracts()
        status = update()
        send_update_mail(status, Config.LOG_UPDATE_NBB)
    except Exception as e:
        update_nbb_logger.error(
            'The following error caused the script to stop'
        )
        update_nbb_logger.exception(e)
        sys.exit()


if __name__ == "__main__":
    main()
