import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    """A class to hold all configuration variables."""

    # Debug
    DEBUG = bool(os.getenv("DEBUG")) == '1'

    # Database module
    DB_USERNAME = os.getenv("DB_USERNAME", "")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_HOST = os.getenv("DB_HOST", "")
    DB_PORT = int(os.getenv("DB_PORT", ""))
    DB_NBB = os.getenv("DB_NBB", "")
    DB_ARCHIVE = os.getenv("DB_ARCHIVE", "")
    CBE_DB = os.getenv("CBE_DB", "")

    # Logger module
    LOG_CONFIG = os.getenv("LOG_CONFIG", "")
    LOG_UPDATE_CBE = os.getenv("LOG_UPDATE_CBE", "")
    LOG_UPDATE_NBB = os.getenv("LOG_UPDATE_NBB", "")
    LOG_MAIL = os.getenv("LOG_MAIL", "")

    # Mailer module
    MAIL_HOST = os.getenv("MAIL_HOST", "")
    MAIL_PORT = int(os.getenv("MAIL_PORT", 1))
    MAIL_USER = os.getenv("MAIL_USER", "")
    MAIL_PASSWORD = os.getenv("MAIL_PASSWORD", "")
    MAIL_RECEIVER = os.getenv("MAIL_RECEIVER", "")

    # CBE updater module
    USER_AGENT = os.getenv("USER_AGENT", "")
    CBE_USER = os.getenv("CBE_USER", "")
    CBE_PASSWORD = os.getenv("CBE_PASSWORD", "")
    URL_LOGIN = os.getenv("URL_LOGIN", "")
    URL_ZIP = os.getenv("URL_ZIP", "")
    ZERO_DATE = os.getenv("ZERO_DATE", "")
    ZERO_POINT = os.getenv("ZERO_POINT", "")
    ZIP_DESTINATION = os.getenv("ZIP_DESTINATION", "")
    ZIP_FILENAME = os.getenv("ZIP_FILENAME", "")

    # NBB updater module
    API_KEY_AUTH = os.getenv("API_KEY_AUTH", "")
    API_KEY_EXTR = os.getenv("API_KEY_EXTR", "")
