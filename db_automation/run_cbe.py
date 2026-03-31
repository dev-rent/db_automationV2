from datetime import date, timedelta

from db_automation.config import Config
from db_automation.mailer.send_mail import send_update_mail
from db_automation.logger.config import update_cbe_logger
from db_automation.updater.cbe.handlers import (
    clean_up, zipbot, preprocess_cbe_data, truncate_db, populate_db
)


def main():
    update_cbe_logger.info('Starting process...')

    try:
        zipbot(date_=date.today() - timedelta(days=1))
        preprocess_cbe_data(path=Config.ZIP_DESTINATION)

        table_dct = truncate_db(db=Config.CBE_DB)
        populate_db(table_dct, db=Config.CBE_DB)
        success = True
    except Exception as e:
        update_cbe_logger.exception(e)
        success = False

    clean_up()
    send_update_mail(success, Config.LOG_UPDATE_CBE)


if __name__ == "__main__":
    main()
