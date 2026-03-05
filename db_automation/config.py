import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    """A class to hold all configuration variables."""

    # Debug
    DEBUG = bool(str(os.getenv("DEBUG")) == '1')

    # Database module
    DB_USERNAME = str(os.getenv("DB_USERNAME", ""))
    DB_PASSWORD = str(os.getenv("DB_PASSWORD", ""))
    DB_HOST = str(os.getenv("DB_HOST", ""))
    DB_PORT = int(os.getenv("DB_PORT", ""))
    DB_NBB = str(os.getenv("DB_NBB"))
    DB_ARCHIVE = str(os.getenv("DB_ARCHIVE"))

    # Logger module
    LOG_CONFIG = str(os.getenv("LOG_CONFIG", ""))
    LOG_UPDATE_CBE = str(os.getenv("LOG_UPDATE_CBE", ""))
    LOG_UPDATE_NBB = str(os.getenv("LOG_UPDATE_NBB", ""))
    LOG_MAIL = str(os.getenv("LOG_MAIL", ""))

    # Mailer module
    MAIL_HOST = str(os.getenv("MAIL_HOST", ""))
    MAIL_PORT = int(os.getenv("MAIL_PORT", 1))
    MAIL_USER = str(os.getenv("MAIL_USER", ""))
    MAIL_PASSWORD = str(os.getenv("MAIL_PASSWORD", ""))
    MAIL_RECEIVER = str(os.getenv("MAIL_RECEIVER", ""))

    # CBE updater module
    USER_AGENT = str(os.getenv("USER_AGENT", ""))
    CBE_USER = str(os.getenv("CBE_USER", ""))
    CBE_PASSWORD = str(os.getenv("CBE_PASSWORD", ""))
    URL_LOGIN = str(os.getenv("URL_LOGIN", ""))
    URL_ZIP = str(os.getenv("URL_ZIP", ""))
    ZERO_DATE = str(os.getenv("ZERO_DATE", ""))
    ZERO_POINT = str(os.getenv("ZERO_POINT", ""))
    ZIP_DESTINATION = str(os.getenv("ZIP_DESTINATION", ""))
    ZIP_FILENAME = str(os.getenv("ZIP_FILENAME", ""))

    # NBB updater module
    API_KEY_AUTH = str(os.getenv("API_KEY_AUTH", ""))
    API_KEY_EXTR = str(os.getenv("API_KEY_EXTR", ""))
