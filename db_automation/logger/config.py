import logging
import logging.config

from db_automation.config import Config

logging.config.fileConfig(Config.LOG_CONFIG)

update_cbe_logger = logging.getLogger("updateCbeLogger")
update_nbb_logger = logging.getLogger("updateNbbLogger")
mail_logger = logging.getLogger("mailLogger")
