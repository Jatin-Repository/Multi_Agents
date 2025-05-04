import os
import time
import smtplib
import pandas as pd
import logging
from dotenv import load_dotenv
from email.mime.text import MIMEText
from langgraph.graph import StateGraph

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL")  # For failure notifications

# WatchdogAgent monitors folder or S3
class WatchdogAgent:
    def __init__(self, folder_path):
        self.folder_path = folder_path

    def __call__(self, state):
        try:
            files = [f for f in os.listdir(self.folder_path) if f.endswith(('.csv', '.xlsx'))]
            reports = []
            for file in files:
                file_path = os.path.join(self.folder_path, file)
                df = pd.read_csv(file_path) if file.endswith('.csv') else pd.read_excel(file_path)
                reports.append({"file": file, "records": len(df)})
            logger.info(f"WatchdogAgent found {len(reports)} file(s) to report.")
            return {"reports": reports, "status": "checked"}
        except Exception as e:
            logger.error(f"WatchdogAgent error: {e}")
            return {"reports": [], "status": "error"}

# ReceiverAgent passes data forward
class ReceiverAgent:
    def __call__(self, state):
        logger.info("ReceiverAgent passing data forward.")
        return state

# ClassifierAgent checks and sends email
class ClassifierAgent:
    def __call__(self, state):
        try:
            for report in state["reports"]:
                status = "good to go" if report["records"] > 0 else "empty file"
                self.send_email(report["file"], status)
            logger.info("ClassifierAgent completed email notifications.")
            return {"status": "notifications sent"}
        except Exception as e:
            logger.error(f"ClassifierAgent error: {e}")
            return {"status": "error"}

    def send_email(self, filename, status):
        subject = f"File Check Result: {filename}"
        body = f"The file {filename} has been processed and is {status}."
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = "recipient@example.com"  # Replace with actual recipient

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, ["recipient@example.com"], msg.as_string())
        logger.info(f"Email sent for {filename} with status {status}")

# Build the graph using a basic dictionary schema
graph = StateGraph()
graph.add_node("watchdog", WatchdogAgent(folder_path="./watch_folder"))
graph.add_node("receiver", ReceiverAgent())
graph.add_node("classifier", ClassifierAgent())

graph.set_entry_point("watchdog")
graph.add_edge("watchdog", "receiver")
graph.add_edge("receiver", "classifier")
graph.set_finish_point("classifier")

app = graph.compile()

# Run the graph periodically (every 10 minutes)
if __name__ == "__main__":
    logger.info("Starting Watchdog Agent Pipeline with scheduler...")
    while True:
        try:
            logger.info("Triggering pipeline run...")
            app.invoke({})
            logger.info("Pipeline run completed. Waiting 10 minutes before next check...")
            time.sleep(600)  # 600 seconds = 10 minutes
        except Exception as e:
            logger.error(f"Pipeline encountered an error: {e}")
            send_failure_email(f"Pipeline encountered an error: {e}")
            time.sleep(600)  # Wait before retrying

    def send_failure_email(error_message):
        subject = "Pipeline Failure Alert"
        body = f"The following error occurred in the pipeline: {error_message}"
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = ADMIN_EMAIL  # The administrator or responsible party

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, [ADMIN_EMAIL], msg.as_string())
        logger.info(f"Failure email sent to {ADMIN_EMAIL}")
