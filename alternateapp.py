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
import seaborn as sns
import time
import requests
import re
import json
from datetime import datetime
import io

from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend

import cortex_chat
##This alternate option for the app has more fucntionality but is incredibly bloated so its more interesting than usefull
matplotlib.use('Agg')
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

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

# In-memory storage for chat history (in production, use a database)
chat_history = {}

def get_user_history(user_id):
    """Get chat history for a specific user."""
    return chat_history.get(user_id, [])

def add_to_history(user_id, query, response):
    """Add a query and response to user's chat history."""
    if user_id not in chat_history:
        chat_history[user_id] = []
    
    chat_history[user_id].append({
        'timestamp': datetime.now().isoformat(),
        'query': query,
        'response': response
    })
    
    # Keep only last 50 conversations per user
    if len(chat_history[user_id]) > 50:
        chat_history[user_id] = chat_history[user_id][-50:]

def build_home_tab():
    """Build the home tab view with navigation."""
    return {
        "type": "home",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🏠 Data Intelligence Assistant"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Welcome to your AI-powered data assistant!* ❄️\n\nI'm connected to Snowflake and ready to help you analyze your data with natural language queries."
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*🚀 Quick Actions*"
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "💬 Start Chat"
                        },
                        "style": "primary",
                        "action_id": "start_chat"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "📊 View History"
                        },
                        "action_id": "view_history"
                    }
                ]
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*💡 Example Queries*"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "• `What are the top 10 movie theatres this week?`\n• `Show me customer support tickets by service type`\n• `What data sources are available?`\n• `Generate a sales report for last month`"
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "💡 *Tip:* You can ask questions directly in our 1-on-1 chat, and I'll provide visualizations and Excel exports when applicable!"
                    }
                ]
            }
        ]
    }

def build_history_tab(user_id):
    """Build the history tab view."""
    history = get_user_history(user_id)
    
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "📊 Query History"
            }
        }
    ]
    
    if not history:
        blocks.extend([
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "No queries yet! Start a conversation to see your history here."
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "🏠 Back to Home"
                        },
                        "action_id": "back_to_home"
                    }
                ]
            }
        ])
    else:
        # Show recent queries (last 10)
        recent_history = history[-10:]
        
        for i, entry in enumerate(reversed(recent_history)):
            timestamp = datetime.fromisoformat(entry['timestamp']).strftime("%m/%d %H:%M")
            query_preview = entry['query'][:100] + "..." if len(entry['query']) > 100 else entry['query']
            
            blocks.extend([
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{timestamp}*\n{query_preview}"
                    },
                    "accessory": {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Rerun"
                        },
                        "value": entry['query'],
                        "action_id": f"rerun_query_{len(recent_history)-i-1}"
                    }
                }
            ])
            
            if i < len(recent_history) - 1:
                blocks.append({"type": "divider"})
        
        blocks.extend([
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "🗑️ Clear History"
                        },
                        "style": "danger",
                        "action_id": "clear_history"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "🏠 Back to Home"
                        },
                        "action_id": "back_to_home"
                    }
                ]
            }
        ])
    
    return {"type": "home", "blocks": blocks}

@app.event("app_home_opened")
def update_home_tab(client, event, logger):
    """Handle app home tab opening."""
    try:
        client.views_publish(
            user_id=event["user"],
            view=build_home_tab()
        )
    except Exception as e:
        logger.error(f"Error publishing App Home: {e}")

@app.action("start_chat")
def handle_start_chat(ack, body, client):
    """Handle start chat button."""
    ack()
    user_id = body["user"]["id"]
    
    try:
        # Open a DM with the user
        dm_response = client.conversations_open(users=user_id)
        channel_id = dm_response["channel"]["id"]
        
        client.chat_postMessage(
            channel=channel_id,
            text="Hi there! 👋 I'm ready to help you with your data questions. What would you like to know?"
        )
    except Exception as e:
        print(f"Error starting chat: {e}")

@app.action("view_history")
def handle_view_history(ack, body, client):
    """Handle view history button."""
    ack()
    user_id = body["user"]["id"]
    
    try:
        client.views_publish(
            user_id=user_id,
            view=build_history_tab(user_id)
        )
    except Exception as e:
        print(f"Error viewing history: {e}")

@app.action("back_to_home")
def handle_back_to_home(ack, body, client):
    """Handle back to home button."""
    ack()
    user_id = body["user"]["id"]
    
    try:
        client.views_publish(
            user_id=user_id,
            view=build_home_tab()
        )
    except Exception as e:
        print(f"Error going back to home: {e}")

@app.action("clear_history")
def handle_clear_history(ack, body, client):
    """Handle clear history button."""
    ack()
    user_id = body["user"]["id"]
    
    if user_id in chat_history:
        del chat_history[user_id]
    
    try:
        client.views_publish(
            user_id=user_id,
            view=build_history_tab(user_id)
        )
    except Exception as e:
        print(f"Error clearing history: {e}")

@app.action(re.compile("rerun_query_.*"))
def handle_rerun_query(ack, body, client):
    """Handle rerun query button."""
    ack()
    user_id = body["user"]["id"]
    query = body["actions"][0]["value"]
    
    try:
        # Open a DM and send the query
        dm_response = client.conversations_open(users=user_id)
        channel_id = dm_response["channel"]["id"]
        
        # Process the query as if it was a new message
        client.chat_postEphemeral(
            channel=channel_id, 
            user=user_id, 
            text=":snowflake: Re-running your query... Looking up the answer for you."
        )
        
        response = CORTEX_APP.chat(query)
        display_agent_response(channel_id, response, lambda **kwargs: client.chat_postMessage(**kwargs))
        
        # Add to history
        add_to_history(user_id, query, response)
        
    except Exception as e:
        print(f"Error rerunning query: {e}")

@app.event("message")
def handle_message_events(ack, body, say, client):
    """Handle incoming messages."""
    ack()
    if 'bot_id' in body['event']:
        return
    
    user_id = body['event']['user']
    channel_id = body['event']['channel']
    prompt = body['event']['text']
    
    print(f"\n--- Received DM: '{prompt}' from User: {user_id} ---")
    
    try:
        # Show thinking message with rotating messages
        thinking_messages = [
            ":snowflake: Thinking... I'm analyzing your question.",
            ":mag: Looking through the data for you...",
            ":chart_with_upward_trend: Preparing your insights...",
            ":brain: Processing your request with AI..."
        ]
        import random
        thinking_msg = random.choice(thinking_messages)
        
        client.chat_postEphemeral(channel=channel_id, user=user_id, text=thinking_msg)
        print("--- Posted ephemeral 'thinking' message ---")
        
        print("--- Calling Cortex Agent... ---")
        response = CORTEX_APP.chat(prompt)
        print(f"--- Cortex Agent Response: {response} ---")
        
        display_agent_response(channel_id, response, say)
        
        # Add to user's history
        add_to_history(user_id, prompt, response)
        
        print("--- Successfully displayed agent response in Slack ---")
        
    except Exception as e:
        error_info = f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}"
        print(f"--- ERROR in handle_message_events: {error_info} ---")
        say(channel=channel_id, text=f"I encountered an error processing your request: {error_info}")

@app.action(re.compile("feedback_(helpful|not_helpful)"))
def handle_feedback(ack, body, client, logger):
    """Handle feedback buttons."""
    ack()
    user = body['user']['id']
    action_id = body['actions'][0]['action_id'] 
    
    feedback_type = "helpful" if "helpful" in action_id else "not helpful"
    print(f"--- Received feedback from User {user}: '{feedback_type}' ---")
    
    # Update the message to show feedback was received
    try:
        client.chat_postMessage(
            channel=body['channel']['id'],
            text=f"✅ Thank you for your feedback! Your input helps me improve.",
            thread_ts=body.get('message', {}).get('ts')
        )
    except Exception as e:
        logger.error(f"Error posting feedback confirmation: {e}")

@app.action("download_excel")
def handle_excel_download(ack, body, client):
    """Handle Excel download button."""
    ack()
    
    try:
        # Get the SQL query from the button value
        sql_query = body['actions'][0]['value']
        df = pd.read_sql(sql_query, CONN)
        
        # Create Excel file in memory
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Data', index=False)
            
            # Add some formatting
            worksheet = writer.sheets['Data']
            for column in worksheet.columns:
                max_length = 0
                column = [cell for cell in column]
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = (max_length + 2)
                worksheet.column_dimensions[column[0].column_letter].width = adjusted_width
        
        excel_buffer.seek(0)
        
        # Upload to Slack
        client.files_upload_v2(
            channel=body['channel']['id'],
            file=excel_buffer.read(),
            filename=f"data_export_{int(time.time())}.xlsx",
            title="Excel Data Export",
            initial_comment="📊 Here's your data exported to Excel format!"
        )
        
    except Exception as e:
        print(f"Error creating Excel file: {e}")
        client.chat_postMessage(
            channel=body['channel']['id'],
            text="❌ Sorry, I couldn't generate the Excel file. Please try again."
        )

def display_agent_response(channel_id, content, say):
    """
    Enhanced display function with improved visuals and Excel export.
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
        
        # Format the data display
        if len(df) > 10:
            preview_df = df.head(10)
            data_preview = f"*Answer (showing first 10 of {len(df)} rows):*\n```{preview_df.to_string(index=False)}```\n_... and {len(df) - 10} more rows_"
        else:
            data_preview = f"*Answer:*\n```{df.to_string(index=False)}```"
        
        fallback_text = f"Query Result: {df.to_string()}"
        
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": data_preview
            }
        })
        
        # Add Excel download button for SQL results
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "📊 *Export Options*"
            },
            "accessory": {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "📥 Download Excel"
                },
                "style": "primary",
                "value": sql,
                "action_id": "download_excel"
            }
        })
        
    # Handle text-only responses
    else:
        answer_text = content.get('text', 'No text content found.')
        fallback_text = f"Answer: {answer_text}"
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Answer:*\n{answer_text}"
            }
        })

    # Add enhanced AI disclaimer and feedback buttons
    blocks.extend([
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "🤖 *AI Generated Response* | Please review and verify the results"
                }
            ]
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "👍 Helpful"
                    },
                    "style": "primary",
                    "value": "helpful",
                    "action_id": "feedback_helpful"
                },
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "👎 Not Helpful"
                    },
                    "value": "not_helpful",
                    "action_id": "feedback_not_helpful"
                }
            ]
        }
    ])
    
    say(channel=channel_id, text=fallback_text, blocks=blocks)

    # Enhanced chart generation for SQL results
    if content.get('sql'):
        df = pd.read_sql(content['sql'], CONN)
        if len(df.columns) >= 2 and len(df) > 0:
            chart_files = create_enhanced_charts(df)
            
            for chart_file in chart_files:
                if chart_file:
                    app.client.files_upload_v2(
                        channel=channel_id,
                        file=chart_file['path'],
                        title=chart_file['title'],
                        initial_comment=chart_file['comment'],
                    )
                    print(f"--- Uploaded {chart_file['title']} to channel {channel_id} ---")
                    os.remove(chart_file['path'])

def create_enhanced_charts(df):
    """Create multiple enhanced chart types based on data characteristics."""
    charts = []
    timestamp = int(time.time())
    
    try:
        # Set up the style
        plt.style.use('seaborn-v0_8-whitegrid')
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', 
                 '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
        
        # Determine chart types based on data
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
        
        # Chart 1: Main visualization based on data structure
        if len(categorical_cols) >= 1 and len(numeric_cols) >= 1:
            fig, ax = plt.subplots(figsize=(12, 8))
            fig.patch.set_facecolor('#f8f9fa')
            
            cat_col = categorical_cols[0]
            num_col = numeric_cols[0]
            
            # Create horizontal bar chart for better readability
            data_for_chart = df.groupby(cat_col)[num_col].sum().sort_values(ascending=True)
            bars = ax.barh(range(len(data_for_chart)), data_for_chart.values, 
                          color=colors[:len(data_for_chart)])
            
            # Enhance the chart
            ax.set_yticks(range(len(data_for_chart)))
            ax.set_yticklabels(data_for_chart.index, fontsize=10)
            ax.set_xlabel(num_col.replace('_', ' ').title(), fontsize=12, fontweight='bold')
            ax.set_title(f'{num_col.replace("_", " ").title()} by {cat_col.replace("_", " ").title()}', 
                        fontsize=16, fontweight='bold', pad=20)
            
            # Add value labels on bars
            for i, (bar, value) in enumerate(zip(bars, data_for_chart.values)):
                ax.text(bar.get_width() + max(data_for_chart.values) * 0.01, 
                       bar.get_y() + bar.get_height()/2, 
                       f'{value:,.0f}', ha='left', va='center', fontsize=9)
            
            # Style improvements
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.grid(True, alpha=0.3, axis='x')
            
            plt.tight_layout()
            chart_path = f'enhanced_chart_{timestamp}_1.png'
            plt.savefig(chart_path, dpi=300, bbox_inches='tight', facecolor='#f8f9fa')
            plt.close()
            
            charts.append({
                'path': chart_path,
                'title': 'Primary Data Visualization',
                'comment': f'📊 Here\'s a detailed view of your {num_col.replace("_", " ").lower()} data'
            })
        
        # Chart 2: Pie chart if appropriate (categorical data with reasonable number of categories)
        if len(categorical_cols) >= 1 and len(numeric_cols) >= 1 and len(df) <= 20:
            fig, ax = plt.subplots(figsize=(10, 8))
            fig.patch.set_facecolor('#f8f9fa')
            
            cat_col = categorical_cols[0]
            num_col = numeric_cols[0]
            
            pie_data = df.groupby(cat_col)[num_col].sum()
            
            # Create pie chart with enhanced styling
            wedges, texts, autotexts = ax.pie(pie_data.values, labels=pie_data.index, 
                                             autopct='%1.1f%%', startangle=90, 
                                             colors=colors[:len(pie_data)],
                                             explode=[0.05] * len(pie_data),
                                             shadow=True)
            
            # Enhance text
            for text in texts:
                text.set_fontsize(10)
                text.set_fontweight('bold')
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
                autotext.set_fontsize(9)
            
            ax.set_title(f'Distribution: {num_col.replace("_", " ").title()}', 
                        fontsize=16, fontweight='bold', pad=20)
            
            plt.tight_layout()
            chart_path = f'enhanced_chart_{timestamp}_2.png'
            plt.savefig(chart_path, dpi=300, bbox_inches='tight', facecolor='#f8f9fa')
            plt.close()
            
            charts.append({
                'path': chart_path,
                'title': 'Distribution Chart',
                'comment': '🥧 Here\'s the percentage breakdown of your data'
            })
        
        # Chart 3: Summary statistics if we have numeric data
        if len(numeric_cols) >= 1:
            fig, ax = plt.subplots(figsize=(10, 6))
            fig.patch.set_facecolor('#f8f9fa')
            
            # Create summary statistics visualization
            summary_stats = df[numeric_cols].describe()
            
            # Create a heatmap-style visualization of key statistics
            key_stats = ['mean', 'std', 'min', 'max']
            available_stats = [stat for stat in key_stats if stat in summary_stats.index]
            
            if len(available_stats) > 0:
                plot_data = summary_stats.loc[available_stats]
                
                # Create grouped bar chart for statistics
                x = range(len(plot_data.columns))
                width = 0.8 / len(available_stats)
                
                for i, stat in enumerate(available_stats):
                    offset = width * (i - len(available_stats)/2 + 0.5)
                    ax.bar([pos + offset for pos in x], plot_data.loc[stat], 
                          width=width, label=stat.title(), alpha=0.8, 
                          color=colors[i % len(colors)])
                
                ax.set_xlabel('Columns', fontsize=12, fontweight='bold')
                ax.set_ylabel('Values', fontsize=12, fontweight='bold')
                ax.set_title('Summary Statistics Overview', fontsize=16, fontweight='bold', pad=20)
                ax.set_xticks(x)
                ax.set_xticklabels([col.replace('_', ' ').title() for col in plot_data.columns], 
                                  rotation=45, ha='right')
                ax.legend()
                ax.grid(True, alpha=0.3, axis='y')
                
                plt.tight_layout()
                chart_path = f'enhanced_chart_{timestamp}_3.png'
                plt.savefig(chart_path, dpi=300, bbox_inches='tight', facecolor='#f8f9fa')
                plt.close()
                
                charts.append({
                    'path': chart_path,
                    'title': 'Summary Statistics',
                    'comment': '📈 Key statistics overview of your numeric data'
                })
    
    except Exception as e:
        print(f"--- ERROR creating enhanced charts: {e} ---")
        
        # Fallback to simple chart
        try:
            plt.figure(figsize=(10, 6))
            if len(df.columns) >= 2:
                df_plot = df.head(20)  # Limit to 20 rows for readability
                plt.bar(range(len(df_plot)), df_plot.iloc[:, 1], color=colors[0])
                plt.xlabel(df.columns[0].replace('_', ' ').title())
                plt.ylabel(df.columns[1].replace('_', ' ').title())
                plt.title('Data Overview')
                plt.xticks(range(len(df_plot)), df_plot.iloc[:, 0], rotation=45)
                plt.tight_layout()
                
                chart_path = f'fallback_chart_{timestamp}.png'
                plt.savefig(chart_path, dpi=300, bbox_inches='tight')
                plt.close()
                
                charts.append({
                    'path': chart_path,
                    'title': 'Data Visualization',
                    'comment': '📊 Your data visualization'
                })
        except Exception as fallback_error:
            print(f"--- ERROR in fallback chart creation: {fallback_error} ---")
    
    return charts

def init():
    """Initialize connections - unchanged from original."""
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
    print("🚀 Enhanced Slack Data Intelligence Assistant is running!")
    handler.start()