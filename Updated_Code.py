import os
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
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

        # Multiple time the notification send
        
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
                state["next"] = ["watchdog"] # only csv and excel
                return state

            latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(self.folder_path, f)))
            file_path = os.path.join(self.folder_path, latest_file)
            df = pd.read_csv(file_path) if latest_file.endswith('.csv') else pd.read_excel(file_path)
            
            if  df.empty == True or df.isnull().all().all() == True: # Columns Present but no data
                state["next"] = ["watchdog"]
                report = {"file": latest_file, "records": len(df), "data_status":0} 
            
            elif df.isnull().any().any() == True:  # Data but few , Null value exist in the dataframe
                length = len(df)
                state["next"] = ["classifier","training"]
                bool_row = pd.isnull(df).any(axis=1)
                drop_index = []
                for i in len(bool_row):
                    if bool_row[i] == True:
                        drop_index.append(i)
                revised_df = pd.DataFrame(index=drop_index,columns=df.columns) # need to send back to bank to revisit the record
                training_df = df.drop(index=drop_index).reset_index(drop=False) # to move into preprocessing 
                training_df.to_csv(file_path,index=False)
                revised_df.to_csv('C:/Users/jatin/Desktop/Multi_Agents/Revised_Folder/{latest_file}',index=False)
                if len(training_df)/ len(df) > 0.7:
                    state["next"] = ["classifier","preprocessing"]
                    report = {"file":latest_file,"records":len(revised_df),"data_status":1,"file_path":file_path}
                else:
                    state["next"] = ["classifier","datadog"]
                    report = {"file":latest_file,"records":len(df),"data_status":2,"file_path":f'C:/Users/jatin/Desktop/Multi_Agents/Revised_Folder/{latest_file}'}
            else:
                state["next"] = ["classifier","preprocessing"]
                report = {"file": latest_file, "records": len(df),"data_status":3,"file_path":file_path}
            
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
            attachment = pd.read_csv(state['file_path'])
            report = state.get("status", [])
            if report["status"] == 0:
                status = "The file is empty and can't to used for Modelling"
            elif report["status"] == 1:
                status = "The file contain minimum required data for modelling"
            elif report["status"] == 2:
                status = "The file doenot contain minimum required data for modelling"
            for report in state.get("reports", []):
                status = "good to go" if report["records"] > 0 else "empty file"
                self.send_email(attachment,report["file"], status)
            logging.info("ClassifierAgent completed notifications")
            state["classifier_state"] = "notifications sent"
            return state
        except Exception as e:
            logging.error(f"ClassifierAgent error: {e}")
            state["classifier_state"] = f"error: {e}"
            return state
    def send_email(self,attachment, filename, status):
        subject = f"File Check Result: {filename}"
        body = f"The file {filename} has been processed and is {status}."
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = SENDER_EMAIL_ADDRESS  # Replace with actual recipient
        msg.attach(MIMEText(body, 'plain'))

        with open(attachment, "rb") as attachment:
            part = MIMEBase('text', 'csv')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f"attachment; filename={os.path.basename(attachment)}")
            msg.attach(part)

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, SENDER_EMAIL_ADDRESS, msg.as_string())
        logger.info(f"Email sent for {filename} with status {status}")
    