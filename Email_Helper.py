import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

# --------- Load Environment Variables --------- #
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")  # your Gmail or SMTP email
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")  # your app-specific password or SMTP password

# --------- Send Email Function --------- #
def send_email(recipient, subject, body):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = recipient
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.send_message(msg)
        print(f"[Email Sent] To: {recipient}, Subject: {subject}")
    except Exception as e:
        print(f"[Email Error] Failed to send email: {e}")

