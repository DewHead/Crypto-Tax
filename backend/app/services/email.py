import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

async def send_notification_email(subject: str, body: str, to_email: str):
    if not settings.SMTP_HOST:
        logger.warning("SMTP_HOST not configured, skipping email notification.")
        return

    if not to_email:
        logger.warning("No recipient email provided, skipping email notification.")
        return

    msg = MIMEMultipart()
    msg['From'] = settings.SMTP_FROM_EMAIL
    msg['To'] = to_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    try:
        # Since smtplib is blocking, in a production FastAPI app we might want to use 
        # a thread pool or an async email library like aiosmtplib.
        # For simplicity and given it's run in BackgroundTasks, we'll use standard smtplib.
        server = smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT)
        if settings.SMTP_USERNAME and settings.SMTP_PASSWORD:
            server.starttls()
            server.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
        
        server.send_message(msg)
        server.quit()
        logger.info(f"Email notification sent to {to_email}")
    except Exception as e:
        logger.error(f"Failed to send email notification: {e}")
