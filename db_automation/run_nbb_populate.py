from db_automation.config import Config
from db_automation.mailer.send_mail import send_update_mail
from db_automation.logger.config import update_nbb_logger
from db_automation.updater.nbb.handlers import (
    prepare_update, populate_destination
)


def main():
    update_nbb_logger.info('Starting process...')

    try:
        # Fetch country codes
        # country_codes = fetch_country_codes(db=Config.DB_NBB)
        
        # Fetch IDs to update
        ids_to_update = prepare_update(
            db_from=Config.DB_ARCHIVE,
            db_to=Config.DB_NBB
        )
        # ids_to_update = {"0701923177"}
        # Update/populate destination
        populate_destination(
            db_from=Config.DB_ARCHIVE,
            db_to=Config.DB_NBB,
            to_update=ids_to_update
        )
        success = True
    except Exception as e:
        update_nbb_logger.exception(e)
        success = False

    send_update_mail(success, Config.LOG_UPDATE_NBB)


if __name__ == "__main__":
    main()
