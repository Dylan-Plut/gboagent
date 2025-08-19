from typing import Any
import os
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import snowflake.connector
import pandas as pd
from snowflake.core import Root
from dotenv import load_dotenv
import matplotlib
import matplotlib.pyplot as plt 
from snowflake.snowpark import Session
import numpy as np
import cortex_chat
import time
import requests

# --- START OF THE FIX ---
# Import the specific cryptography functions needed to decrypt the key manually.
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
# --- END OF THE FIX ---

matplotlib.use('Agg')
load_dotenv()

# These variables load correctly from your .env file
ACCOUNT = os.getenv("ACCOUNT")
HOST = os.getenv("HOST")
USER = os.getenv("USER")
DATABASE = os.getenv("DATABASE")
SCHEMA = os.getenv("SCHEMA")
ROLE = os.getenv("ROLE")
WAREHOUSE = os.getenv("WAREHOUSE")
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
AGENT_ENDPOINT = os.getenv("AGENT_ENDPOINT")
SEMANTIC_MODEL = os.getenv("SEMANTIC_MODEL")
RSA_PRIVATE_KEY_PATH = os.getenv("RSA_PRIVATE_KEY_PATH")
RSA_PRIVATE_KEY_PASSWORD = os.getenv("RSA_PRIVATE_KEY_PASSWORD")
MODEL = os.getenv("MODEL")

DEBUG = False

app = App(token=SLACK_BOT_TOKEN)
messages = []

# All Slack event handlers and helper functions remain the same...
@app.message("hello")
def message_hello(message, say):
    build = """
Not a developer was stirring, all deep in the fight.
The code was deployed in the pipelines with care,
In hopes that the features would soon be there.
And execs, so eager to see the results,
Were prepping their speeches, avoiding the gulps.
When, what to my wondering eyes should appear,
But a slide-deck update, with a demo so clear!
And we shouted out to developers,
Let's launch this build live and avoid any crash!
The demos they created, the videos they made,
Were polished and ready, the hype never delayed.
            """
    say(build)
    say(
        text = "Let's BUILD",
        blocks = [
            {
                "type": "header",
                "text": { "type": "plain_text", "text": f":snowflake: Let's BUILD!" }
            },
        ]                
    )

@app.event("message")
def handle_message_events(ack, body, say):
    try:
        ack()
        prompt = body['event']['text']
        say(
            text="Snowflake Cortex AI is generating a response",
            blocks=[
                {"type": "divider"},
                {"type": "section", "text": {"type": "plain_text", "text": ":snowflake: Snowflake Cortex AI is generating a response. Please wait..."}},
                {"type": "divider"},
            ]
        )
        response = ask_agent(prompt)
        display_agent_response(response,say)
    except Exception as e:
        error_info = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
        print(error_info)
        say(
            text="Request failed...",
            blocks=[
                {"type": "divider"},
                {"type": "section", "text": {"type": "plain_text", "text": f"{error_info}"}},
                {"type": "divider"},
            ]
        )        

def ask_agent(prompt):
    resp = CORTEX_APP.chat(prompt)
    return resp

def display_agent_response(content,say):
    if content['sql']:
        sql = content['sql']
        df = pd.read_sql(sql, CONN)
        say(
            text="Answer:",
            blocks=[
                {"type": "rich_text", "elements": [{"type": "rich_text_quote", "elements": [{"type": "text", "text": "Answer:", "style": {"bold": True}}]}, {"type": "rich_text_preformatted", "elements": [{"type": "text", "text": f"{df.to_string()}"}]}]}
            ]
        )
        if len(df.columns) > 1:
            chart_img_url = None
            try:
                chart_img_url = plot_chart(df)
            except Exception as e:
                error_info = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
                print(f"Warning: Data likely not suitable for displaying as a chart. {error_info}")
            if chart_img_url is not None:
                say(text="Chart", blocks=[{"type": "image", "title": {"type": "plain_text", "text": "Chart"}, "block_id": "image", "slack_file": {"url": f"{chart_img_url}"}, "alt_text": "Chart"}])
    else:
        say(
            text="Answer:",
            blocks = [
                {"type": "rich_text", "elements": [{"type": "rich_text_quote", "elements": [{"type": "text", "text": f"Answer: {content['text']}", "style": {"bold": True}}]}, {"type": "rich_text_quote", "elements": [{"type": "text", "text": f"* Citation: {content['citations']}", "style": {"italic": True}}]}]}
            ]                
        )

def plot_chart(df):
    plt.figure(figsize=(10, 6), facecolor='#333333')
    plt.pie(df[df.columns[1]], labels=df[df.columns[0]], autopct='%1.1f%%', startangle=90, colors=['#1f77b4', '#ff7f0e'], textprops={'color':"white",'fontsize': 16})
    plt.axis('equal')
    plt.gca().set_facecolor('#333333')   
    plt.tight_layout()
    file_path_jpg = 'pie_chart.jpg'
    plt.savefig(file_path_jpg, format='jpg')
    file_size = os.path.getsize(file_path_jpg)
    file_upload_url_response = app.client.files_getUploadURLExternal(filename=file_path_jpg,length=file_size)
    file_upload_url = file_upload_url_response['upload_url']
    file_id = file_upload_url_response['file_id']
    with open(file_path_jpg, 'rb') as f:
        response = requests.post(file_upload_url, files={'file': f})
    img_url = None
    if response.status_code == 200:
        response = app.client.files_completeUploadExternal(files=[{"id":file_id, "title":"chart"}])
        img_url = response['files'][0]['permalink']
        time.sleep(2)
    return img_url

def init():
    # --- START OF THE NEW AND CORRECT FIX ---
    # Step 1: Manually read and decrypt the private key. We are borrowing the
    # proven, working logic from your JWTGenerator class.
    print(">>>>>>>>>> Manually decrypting private key for database connection...")
    with open(RSA_PRIVATE_KEY_PATH, "rb") as pem_in:
        pemlines = pem_in.read()
    
    private_key_obj = load_pem_private_key(
        pemlines,
        password=RSA_PRIVATE_KEY_PASSWORD.encode(), # The password must be in bytes
        backend=default_backend()
    )
    print(">>>>>>>>>> Private key decrypted successfully.")

    # Step 2: Connect to Snowflake by passing the DECRYPTED KEY OBJECT directly.
    # By doing this, we are no longer asking the connector to read files or handle
    # passwords, which is where it was failing.
    print(">>>>>>>>>> Connecting to Snowflake database using private key object...")
    conn = snowflake.connector.connect(
        user=USER,
        account=ACCOUNT,
        private_key=private_key_obj, # Pass the decrypted key object here
        warehouse=WAREHOUSE,
        role=ROLE,
        host=HOST,
        database=DATABASE,
        schema=SCHEMA
    )
    # --- END OF THE NEW AND CORRECT FIX ---
    
    if conn:
        print(">>>>>>>>>> Snowflake database connection successful!")
    else:
        print(">>>>>>>>>> Snowflake database connection FAILED!")
        exit()

    # The CortexChat class will continue to manage its own separate key decryption
    # for REST API calls. This part is correct and does not need to change.
    cortex_app = cortex_chat.CortexChat(
        agent_url=AGENT_ENDPOINT, 
        semantic_model=SEMANTIC_MODEL,
        model=MODEL, 
        account=ACCOUNT,
        user=USER,
        private_key_path=RSA_PRIVATE_KEY_PATH,
        private_key_password=RSA_PRIVATE_KEY_PASSWORD
    )

    print(">>>>>>>>>> Init complete")
    return conn, cortex_app

# Main execution block
if __name__ == "__main__":
    CONN, CORTEX_APP = init()
    Root = Root(CONN)
    SocketModeHandler(app, SLACK_APP_TOKEN).start()