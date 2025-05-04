import logging
import time
import os
import smtplib
from langgraph.graph import StateGraph, END
from email.mime.text import MIMEText
from Agents import WatchdogAgent,ReceiverAgent,ClassifierAgent
from dotenv import load_dotenv

class PipelineState:
    def __init__(self, reports=None, watchdog_state=None, receiver_state=None, classifier_state=None):
        self.reports = reports or []
        self.watchdog_state = watchdog_state
        self.receiver_state = receiver_state
        self.classifier_state = classifier_state

    def to_dict(self):
        return {
            "reports": self.reports,
            "watchdog_state": self.watchdog_state,
            "receiver_state": self.receiver_state,
            "classifier_state": self.classifier_state
        }

# --------- Load Environment Variables --------- #
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")  # your Gmail or SMTP email
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")  # your app-specific password or SMTP password
SENDER_EMAIL_ADDRESS = os.getenv("SENDER_EMAIL_ADDRESS")
ADMIN_EMAIL = os.getenv('ADMIN_EMAIL')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


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


# Build the graph using a basic dictionary schema
graph = StateGraph(state_schema=dict)
graph.add_node("watchdog", WatchdogAgent(folder_path="./watch_folder"))
graph.add_node("receiver", ReceiverAgent())
graph.add_node("classifier", ClassifierAgent())

graph.set_entry_point("watchdog")
graph.add_edge("watchdog", "receiver")
graph.add_edge("receiver", "classifier")
graph.set_finish_point("classifier")

app = graph.compile()

# Run the graph periodically (every 2 minutes)
if __name__ == "__main__":
    logger.info("Starting Watchdog Agent Pipeline with scheduler...")
    while True:
        try:
            logger.info("Triggering pipeline run...")
            initial_state = PipelineState().to_dict()
            result = app.invoke(initial_state)
            logging.info(f"Pipeline result: {result}")
            logger.info("Pipeline run completed. Waiting 2 minutes before next check...")
            time.sleep(60)  # 120 seconds = 1 minutes
        except Exception as e:
            logger.error(f"Pipeline encountered an error: {e}")
            send_failure_email(f"Pipeline encountered an error: {e}")
            time.sleep(60)  # Wait before retrying