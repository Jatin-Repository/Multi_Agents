import os
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from langchain_openai import ChatOpenAI
from langchain_core.runnables import Runnable
from langgraph.graph import StateGraph
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

import logging
import smtplib

# --------- Load Environment Variables --------- #
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")  # your Gmail or SMTP email
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")  # your app-specific password or SMTP password
SENDER_EMAIL_ADDRESS = os.getenv("SENDER_EMAIL_ADDRESS")


# # Set up logging
# logging.basicConfig(level=logging.INFO, file_name = "pipeline_run.log", filemode='w', format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


# WatchdogAgent monitors folder or S3
class WatchdogAgent: # Data Agent
    def __init__(self, folder_path):
        self.folder_path = folder_path

    def dynamic_create_subfolder(self, parent_folder,subfolder,file_name):
        # Build the full subfolder path
        subfolder_path = os.path.join(parent_folder, subfolder)
        # Ensure the subfolder exists (creates parent + subfolder if missing)
        if not os.path.exists(subfolder_path):
            os.makedirs(subfolder_path, exist_ok=True)
        else:
            logger.info('f Subfolder :{subfolder} already exist.')
        # Build the full file path
        file_path = os.path.join(subfolder_path, file_name)
        return file_path

    def __call__(self, state):
        try:
            files = [f for f in os.listdir(self.folder_path) if f.endswith(('.csv', '.xlsx'))]
            if not files:
                logging.info("WatchdogAgent found no CSV/XLSX files")
                state["watchdog_state"] = "no files found"
                state["reports"] = []
                state["next"] = "watchdog" # only csv and excel
                return state

            latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(self.folder_path, f)))
            file_path = os.path.join(self.folder_path, latest_file)
            df = pd.read_csv(file_path) if latest_file.endswith('.csv') else pd.read_excel(file_path)
            if  df.empty == True or df.isnull().all().all() == True: # Columns Present but no data
                state["next"] = "watchdog"
                report = {"file": latest_file, "records": len(df), "data_status":0} 
                logging.info(f"WatchdogAgent has not picked latest file: {latest_file}")
                state["watchdog_state"] = "unchecked"

            elif df.isnull().any().any() == True:  # Data but few , Null value exist in the dataframe
                bool_row = pd.isnull(df).any(axis=1).to_list()
                drop_index = []
                for i in range(len(bool_row)):
                    if bool_row[i] == True:
                        drop_index.append(i)
                revised_df = pd.DataFrame(df,index=drop_index,columns=df.columns) # need to send back to bank to revisit the record
                training_df = df.drop(index=drop_index).reset_index(drop=True) # to move into preprocessing 
                logger.info(latest_file.split('.')[0])
                updated_file_path = self.dynamic_create_subfolder("Updated",latest_file.split('.')[0],latest_file)
                revised_file_path = self.dynamic_create_subfolder("Revised",latest_file.split('.')[0],latest_file)
                training_df.to_csv(updated_file_path,index=False)
                revised_df.to_csv(revised_file_path,index=False)
                state["next"] = ["classifier","preprocessing"]
                state["watchdog_state"] = "checked"
                report = {"file":latest_file,"records":len(training_df),"data_status":1,"file_path":f"{updated_file_path}","revised_file_path":f'{revised_file_path}'}
                
            else:
                state["next"] = ["classifier","preprocessing"]
                report = {"file": latest_file, "records": len(df),"data_status":2,"file_path":file_path,"revised_file_path":None}
                state["watchdog_state"] = "checked"
            state["reports"] = report
            return state
        except Exception as e:
            logging.error(f"WatchdogAgent error: {e}")
            state["watchdog_state"] = f"error: {e}"
            state["reports"] = []
            return state
    
    

# ClassifierAgent checks and sends email
class ClassifierAgent: # Communication Hub
    def __call__(self, state):
        try:
            logger.info(f"{state.get('reports')}")
            report = state.get('reports')

            if report["data_status"] == 0:
                content = "the file is empty and can't to used for Modelling"

            elif report["data_status"] == 1:
                content = "the file meet the minimum required data for Modelling. Please update the above file and resend it for next iteration."
            
            elif report["data_status"] == 2:
                content = "the file contain data is good to go for Modelling."
            
            state["content"] = content
            
            #logger.info("Sending...")
            self.send_email(report["revised_file_path"],report["file"], content)
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
        msg['To'] = SENDER_EMAIL_ADDRESS 

        # Only attach files if present
        if attachment: # since attachment is dataframe
            msg = MIMEMultipart()
            msg.attach(MIMEText(body, 'plain'))
            if os.path.isfile(attachment):
                with open(attachment, 'rb') as f:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(f.read())
                    encoders.encode_base64(part)
                    filename = os.path.basename(attachment)
                    part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
                    msg.attach(part)
        else:
            msg = MIMEText(body, 'plain')

        
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, SENDER_EMAIL_ADDRESS, msg.as_string())
        logger.info(f"Email sent for {filename} with status {status}")
    


class PreprocessingAgent:
    def __call__(self, state):
        file_path = state.get('reports')['file_path']
        logger.info(f"Preprocessing:{file_path}")
        train_df = pd.read_csv(file_path) if file_path.endswith('.csv') else pd.read_excel(file_path)
        logger.info("Preprocessing File contain "+{len(train_df)}+"records")
        state["preprocessing_state"] = "checked"
        state["next"] = "training"
        return state