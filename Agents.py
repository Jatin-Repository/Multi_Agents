import os
import pandas as pd
from email.mime.text import MIMEText
from langchain_openai import ChatOpenAI
from langchain_core.runnables import Runnable
from Email_Helper import send_email
from langgraph.graph import StateGraph
from dotenv import load_dotenv
import logging
import smtplib

# --------- Load Environment Variables --------- #
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")  # your Gmail or SMTP email
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")  # your app-specific password or SMTP password
SENDER_EMAIL_ADDRESS = os.getenv("SENDER_EMAIL_ADDRESS")


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


# WatchdogAgent monitors folder or S3
class WatchdogAgent:
    def __init__(self, folder_path):
        self.folder_path = folder_path

    def __call__(self, state):
        
        # try:
        #     files = [f for f in os.listdir(self.folder_path) if f.endswith(('.csv', '.xlsx'))]
        #     reports = []
        #     for file in files:
        #         file_path = os.path.join(self.folder_path, file)
        #         df = pd.read_csv(file_path) if file.endswith('.csv') else pd.read_excel(file_path)
        #         reports.append({"file": file, "records": len(df)})
        #     logging.info(f"WatchdogAgent found {len(reports)} report(s)")
        #     state["watchdog_state"] = "checked"
        #     state["reports"] = reports
        #     return state
        # except Exception as e:
        #     logging.error(f"WatchdogAgent error: {e}")
        #     state["watchdog_state"] = f"error: {e}"
        #     state["reports"] = []
        #     return state

        try:
            files = [f for f in os.listdir(self.folder_path) if f.endswith(('.csv', '.xlsx'))]
            if not files:
                logging.info("WatchdogAgent found no CSV/XLSX files")
                state["watchdog_state"] = "no files found"
                state["reports"] = []
                return state

            latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(self.folder_path, f)))
            file_path = os.path.join(self.folder_path, latest_file)
            df = pd.read_csv(file_path) if latest_file.endswith('.csv') else pd.read_excel(file_path)
            report = {"file": latest_file, "records": len(df)}

            logging.info(f"WatchdogAgent picked latest file: {latest_file}")
            state["watchdog_state"] = "checked"
            state["reports"] = [report]
            return state
        except Exception as e:
            logging.error(f"WatchdogAgent error: {e}")
            state["watchdog_state"] = f"error: {e}"
            state["reports"] = []
            return state

# ReceiverAgent passes data forward
class ReceiverAgent:
    def __call__(self, state):
        logging.info("ReceiverAgent forwarding state")
        state["receiver_state"] = "received"
        return state

# ClassifierAgent checks and sends email
class ClassifierAgent: # Communication Hub
    def __call__(self, state):
        try:
            for report in state.get("reports", []):
                status = "good to go" if report["records"] > 0 else "empty file"
                self.send_email(report["file"], status)
                logging.info("ClassifierAgent completed notifications")
                state["classifier_state"] = "notifications sent"
                return state
        except Exception as e:
            logging.error(f"ClassifierAgent error: {e}")
            state["classifier_state"] = f"error: {e}"
            return state
    def send_email(self, filename, status):
        subject = f"File Check Result: {filename}"
        body = f"The file {filename} has been processed and is {status}."
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = SENDER_EMAIL_ADDRESS  # Replace with actual recipient

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, SENDER_EMAIL_ADDRESS, msg.as_string())
        logger.info(f"Email sent for {filename} with status {status}")
    
