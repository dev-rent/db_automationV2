from email.message import EmailMessage
from smtplib import SMTP

from db_automation.config import Config


def send_update_mail(status: bool, log_path: str):
    """Send mail with update log."""

    msg = EmailMessage()
    msg["From"] = Config.MAIL_USER
    msg["To"] = Config.MAIL_RECEIVER

    success = "SUCCESS" if status else "FAILED"

    msg["Subject"] = f"[{success}] - Database update results"

    with open(log_path, mode="r", encoding="utf-8") as f:
        msg.set_content(f.read())

    with SMTP(Config.MAIL_HOST, Config.MAIL_PORT) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(Config.MAIL_USER, Config.MAIL_PASSWORD)
        smtp.send_message(msg)
