###THIS IS THE MODIFIED, NON DEV CODE TO BE EDITED
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
import time
import requests
import re # --- ADDED: Import regular expressions for the action handler

from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend

import cortex_chat

matplotlib.use('Agg')

# Forcing override to ensure .env in this folder is used
load_dotenv(override=True)

# --- ENVIRONMENT VARIABLES ---
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

app = App(token=SLACK_BOT_TOKEN)

@app.event("app_home_opened")
def update_home_tab(client, event, logger):
    # This function is correct, no changes needed
    try:
        client.views_publish(
            user_id=event["user"],
            view={"type": "home", "blocks": [{"type": "header", "text": {"type": "plain_text", "text": "Welcome to your Data Intelligence Assistant! ❄️"}}, {"type": "section", "text": {"type": "mrkdwn", "text": "I am an AI-powered assistant connected to Snowflake, designed to help you get insights from our data. You can ask me questions directly in our 1-on-1 chat."}}, {"type": "section", "text": {"type": "mrkdwn", "text": "*Here are some examples of what you can ask:*\n• `What are the top 10 movie theatres this week?`\n• `Show me a breakdown of customer support tickets by service type.`\n• `What data is available?`"}}, {"type": "divider"}]}
        )
    except Exception as e:
        logger.error(f"Error publishing App Home: {e}")

@app.event("message")
def handle_message_events(ack, body, say, client):
    # This function is correct, no changes needed
    ack()
    if 'bot_id' in body['event']:
        return
    user_id = body['event']['user']
    channel_id = body['event']['channel']
    prompt = body['event']['text']
    print(f"\n--- Received DM: '{prompt}' from User: {user_id} ---")
    try:
        client.chat_postEphemeral(channel=channel_id, user=user_id, text=":snowflake: Thinking... I'm looking up the answer for you.") #we will change this later to rotate through a list of messages.
        print("--- Posted ephemeral 'thinking' message ---")
        print("--- Calling Cortex Agent... ---")
        response = CORTEX_APP.chat(prompt)
        print(f"--- Cortex Agent Response: {response} ---")
        display_agent_response(channel_id, response, say)
        print("--- Successfully displayed agent response in Slack ---")
    except Exception as e:
        error_info = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
        print(f"--- ERROR in handle_message_events: {error_info} ---")
        say(channel=channel_id, text=f"An error occurred: {error_info}")

# --- START OF FIX 1: UPDATE THE ACTION HANDLER ---
# The listener now uses a regular expression to catch both 'feedback_helpful'
# and 'feedback_not_helpful' actions in a single function.
@app.action(re.compile("feedback_(helpful|not_helpful)"))
def handle_feedback(ack, body, say, logger):
    ack()
    user = body['user']['id']
    # The action ID itself tells us what was clicked
    action_id = body['actions'][0]['action_id'] 
    
    # We can get the value from the action_id
    feedback_type = "helpful" if "helpful" in action_id else "not helpful"
    
    print(f"--- Received feedback from User {user}: '{feedback_type}' ---")
    
    # You could add logic here to update the original message, e.g., to remove the buttons
    # For now, just send a confirmation message.
    say(text=f"Thank you for your feedback!", channel=body['channel']['id'])
# --- END OF FIX 1 ---

# In app.py

def display_agent_response(channel_id, content, say):
    """
    Handles both SQL and text responses from the agent, plotting charts
    and following AI app best practices.
    """
    blocks = []
    fallback_text = "Here is the response from your Data Intelligence Assistant."
    
    if not content:
        print("--- WARNING: display_agent_response received empty content. ---")
        say(channel=channel_id, text="I'm sorry, I couldn't generate a response.")
        return

    print(content)
    # Handle SQL responses
    if content.get('sql'):
        sql = content['sql']
        df = pd.read_sql(sql, CONN)
        answer_text = f"*Answer:*\n```{df.to_string()}```"
        fallback_text = f"Query Result: {df.to_string()}"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": answer_text}})
    # Handle text-only responses
    else:
        answer_text = content.get('text', 'No text content found.')
        fallback_text = f"Answer: {answer_text}"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*Answer:*\n{answer_text}"}})

    # Add AI disclaimer and feedback buttons
    blocks.extend([
        {"type": "context", "elements": [{"type": "mrkdwn", "text": "This content was generated by an AI assistant. Please review carefully."}]},
        {"type": "actions", "elements": [
            {"type": "button", "text": {"type": "plain_text", "text": "👍 Helpful"}, "value": "helpful", "action_id": "feedback_helpful"},
            {"type": "button", "text": {"type": "plain_text", "text": "👎 Not Helpful"}, "value": "not_helpful", "action_id": "feedback_not_helpful", "style": "danger"}
        ]}
    ])
    
    say(channel=channel_id, text=fallback_text, blocks=blocks)

    # Plot a chart if the SQL query had results with more than one column
    if content.get('sql'):
        df = pd.read_sql(content['sql'], CONN)
        if len(df.columns) > 1:
            chart_file = plot_chart(df)
            if chart_file:
                app.client.files_upload_v2(
                    channel=channel_id,
                    file=chart_file,
                    title="Data Chart",
                    initial_comment="Here is a visual representation of the data:",
                )
                print(f"--- Uploaded chart to channel {channel_id} ---")
                os.remove(chart_file)

def plot_chart(df):
    """Generates a chart and saves it as a local file."""
    try:
        plt.figure(figsize=(10, 6), facecolor='#333333')
        plt.pie(df[df.columns[1]], labels=df[df.columns[0]], autopct='%1.1f%%', startangle=90, colors=['#1f77b4', "#05ff9f", "#FFFFFF", "#7330CA"], textprops={'color':"white",'fontsize': 16})
        plt.axis('equal')
        plt.gca().set_facecolor("#000000")  # Fixed syntax error here
        plt.tight_layout()
        file_path = f'chart_{int(time.time())}.jpg'
        plt.savefig(file_path, format='jpg')
        plt.close()
        print(f"--- Chart saved to {file_path} ---")
        return file_path
    except Exception as e:
        print(f"--- ERROR creating chart: {e} ---")
        return None

def init():
    # This function is correct, no changes needed
    print(f">>>>>>>>>> Connecting with ROLE: {ROLE} and USER: {USER}")
    print(">>>>>>>>>> Manually decrypting private key for database connection...")
    with open(RSA_PRIVATE_KEY_PATH, "rb") as pem_in:
        pemlines = pem_in.read()
    private_key_obj = load_pem_private_key(pemlines, password=RSA_PRIVATE_KEY_PASSWORD.encode(), backend=default_backend())
    print(">>>>>>>>>> Private key decrypted successfully.")
    print(">>>>>>>>>> Connecting to Snowflake database using private key object...")
    conn = snowflake.connector.connect(user=USER, account=ACCOUNT, private_key=private_key_obj, warehouse=WAREHOUSE, role=ROLE, host=HOST, database=DATABASE, schema=SCHEMA)
    if conn:
        print(">>>>>>>>>> Snowflake database connection successful!")
    else:
        print(">>>>>>>>>> Snowflake database connection FAILED!"); exit()
    
    # Simplified initialization without search service
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

if __name__ == "__main__":
    CONN, CORTEX_APP = init()
    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    print("Bolt app is running!")
    handler.start()