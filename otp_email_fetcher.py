import re
import time
import logging
import imaplib
from datetime import datetime, timedelta, timezone

import email
from email.header import decode_header
from email.utils import parsedate_to_datetime

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def fetch_otp_from_email(username, password, current_time):
    try:
        logger.info("Connecting to email server...")
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(username, password)
        mail.select("inbox")

        # Search for all emails
        logger.info("Searching for emails...")
        status, messages = mail.search(None, "ALL")
        email_ids = messages[0].split()

        if email_ids:
            # Traverse the email list from the most recent to the oldest
            for count, email_id in enumerate(reversed(email_ids), start=1):

                if count > 3:
                    logger.info("Checked the last 3 emails but no OTP email found.")
                    return None

                status, msg_data = mail.fetch(email_id, "(RFC822)")
                msg = email.message_from_bytes(msg_data[0][1])

                # Decode the subject
                subject, encoding = decode_header(msg["Subject"])[0]
                if isinstance(subject, bytes):
                    subject = subject.decode(encoding if encoding else 'utf-8')

                if (
                    "Your OTP for ExtraaEdge CRM login" in subject and
                    parsedate_to_datetime(msg["Date"]) > current_time and
                    msg.is_multipart() and
                    msg.get_content_type() == "multipart/alternative"
                ):
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            try:
                                body = part.get_payload(decode=True).decode('utf-8', errors='ignore')
                                otp_regex = r'<span[^>]*>(\d+)</span>'
                                match = re.search(otp_regex, body)
                                if match:
                                    otp = match.group(1)
                                    logger.info(f"OTP found: {otp}")
                                    return otp
                            except Exception as e:
                                logger.error(f"Error processing email part: {e}")
                    return None  # Return None if no OTP is found

        logger.warning("No OTP email found after the request time.")
        return None 

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return None
    finally:
        mail.logout()
        logger.info("Logged out from email server.")

def get_panel_otp(username, password, current_time=None, timeout=60):
    panel_otp = None
    start_time = time.time()
    attempts = 0 

    while not panel_otp:
        attempts += 1
        logger.info(f"Attempting to fetch OTP (Attempt {attempts})...")
        time.sleep(10)  # Wait for 10 seconds before checking the email
        panel_otp = fetch_otp_from_email(username, password, current_time)
        if time.time() - start_time > timeout:
            logger.warning("Timeout reached while waiting for OTP.")
            break

    return str(panel_otp) if panel_otp else None

if __name__ == '__main__':
    otp_email = "****ts@co****ni*.com"
    otp_password = "********"
    
    # Define the target IST time
    target_ist_time = datetime(2024, 10, 6, 8, 51, 5)
    adjusted_ist_time = target_ist_time - timedelta(minutes=5)
    current_time = adjusted_ist_time - timedelta(hours=5, minutes=30)
    current_time = current_time.replace(tzinfo=timezone.utc)

    user_otp = get_panel_otp(otp_email, otp_password, current_time)
    logger.info(f"OTP received: {user_otp}")
