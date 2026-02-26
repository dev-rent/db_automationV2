import sys

from db_automation.logger.log_config import update_nbb_logger
from db_automation.updater.nbb_archive.update import fetch_extracts, update


def main():
    update_nbb_logger.info('Starting process...')

    try:
        fetch_extracts()
        update()
    except Exception as e:
        update_nbb_logger.error(
            'The following error caused the script to stop'
        )
        update_nbb_logger.exception(e)
        sys.exit()


if __name__ == "__main__":
    main()