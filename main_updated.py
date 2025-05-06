import logging
import time
import platform
import os
import smtplib
from langgraph.graph import StateGraph, END, START
from email.mime.text import MIMEText
from Updated_Agent import WatchdogAgent,ClassifierAgent,PreprocessingAgent
from dotenv import load_dotenv
from typing import Literal
import random
import networkx as nx
import matplotlib.pyplot as plt
import pydot
from networkx.drawing.nx_pydot import graphviz_layout
from IPython.display import Image, display
from langchain_core.runnables.graph import CurveStyle, MermaidDrawMethod, NodeStyles



class PipelineState:
    def __init__(self, reports=None, watchdog_state=None, next=None, classifier_state=None, preprocessing_state =None):
        self.reports = reports or []
        self.watchdog_state = watchdog_state
        self.classifier_state = classifier_state
        self.preprocessing_state = preprocessing_state
        self.next = next

    def to_dict(self):
        return {
            "reports": self.reports,
            "watchdog_state": self.watchdog_state,
            "classifier_state": self.classifier_state,
            "preprocessing_state":self.preprocessing_state,
            "next":self.next
        }

# --------- Load Environment Variables --------- #
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")  # your Gmail or SMTP email
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")  # your app-specific password or SMTP password
SENDER_EMAIL_ADDRESS = os.getenv("SENDER_EMAIL_ADDRESS")
ADMIN_EMAIL = os.getenv('ADMIN_EMAIL')

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    filename="pipeline_run.log",  
    filemode='w',                 # overwrite each run
    format='%(asctime)s [%(levelname)s] %(message)s'
)
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


# # Define the loop condition
# def should_continue(state):
#     return state.get("next")


# Build the graph
graph = StateGraph(state_schema=dict)

graph.add_node("watchdog", WatchdogAgent(folder_path="./watch_folder"))
graph.add_node("classifier", ClassifierAgent())
graph.add_node("preprocessing", PreprocessingAgent())

# Set entry point
graph.add_edge(START,"watchdog")

# Define edges (based on your WatchdogAgent's 'next')
graph.add_edge("watchdog", "classifier")
graph.add_edge("watchdog", "preprocessing")

graph.set_finish_point({"watchdog",'classifier','preprocessing'})


app = graph.compile()

with open("graph.png", "wb") as f:
    f.write(app.get_graph().draw_mermaid_png(draw_method=MermaidDrawMethod.API))


if platform.system() == "Darwin":       # macOS
    os.system("open graph.png")
elif platform.system() == "Windows":    # Windows
    os.system("start graph.png")
else:                                   # Linux
    os.system("xdg-open graph.png")

# Run the graph periodically (every 2 minutes)
if __name__ == "__main__":
    logger.info("Starting Watchdog Agent Pipeline with scheduler...")
    while True:
        try:
            logger.info("Triggering pipeline run...")
            initial_state = PipelineState().to_dict()
            result = app.invoke(initial_state)
            logger.info("Hello")
            logging.info(f"Pipeline result: {result}")
            logger.info("Pipeline run completed. Waiting 2 minutes before next check...")
            time.sleep(60)  # 120 seconds = 1 minutes
        except Exception as e:
            logger.error(f"Pipeline encountered an error: {e}")
            send_failure_email(f"Pipeline encountered an error: {e}")
            time.sleep(60)  # Wait before retrying




























































