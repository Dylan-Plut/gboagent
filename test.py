import os
import json
import requests
from dotenv import load_dotenv
import generate_jwt
from generate_jwt import JWTGenerator
import sys

for k in ["ACCOUNT","DEMO_USER","RSA_PRIVATE_KEY_PATH"]:
    if not os.getenv(k):
        sys.exit(f"Missing env var: {k}")

# Load environment variables from .env
load_dotenv()

# Instantiate JWT generator and get token
jwt = JWTGenerator(os.getenv("ACCOUNT"),os.getenv("DEMO_USER"),os.getenv("RSA_PRIVATE_KEY_PATH"))
jwt_token = jwt.get_token()

# Build the IMAXBOT-specific payload
payload = {
    "model": "claude-3-5-sonnet",
    "preamble": """You are a sophisticated and helpful Data Intelligence Assistant. Your primary purpose is to provide users with accurate insights from the company's data ecosystem. You have access to two distinct, powerful tools to accomplish this:
Cortex Analyst: Use this tool for quantitative questions against structured data. It excels at performing calculations, aggregations (like sum, count, average), trend analysis, and answering questions about specific business metrics, KPIs, tables, or columns.
Cortex Search: Use this tool for qualitative questions and information retrieval from unstructured and semi-structured documents. It is designed to find relevant information within text-based sources like reports, presentations, articles, and knowledge bases.
Your core responsibility is to accurately interpret the user's intent. First, understand what the user is asking for—a specific number or a general explanation. Then, select the appropriate tool to deliver the most precise and relevant answer. If a user's query is ambiguous, proactively ask clarifying questions to ensure you can provide the best possible response.

You must follow a strict decision-making process to route user queries. Analyze every query based on the following rules to select the correct tool.
Analyze User Intent and Keywords:
Trigger Cortex Analyst if the query involves:
Quantitative Language: "How many," "what is the total," "calculate," "sum," "average," "count," "top 5," "what percentage," "compare," "measure."
Structured Data References: Mentions of specific database tables, columns, records, or well-defined business metrics (e.g., "Q3 revenue," "customer churn rate," "daily active users").
The expected answer is a number, chart, or a precise data point.
Trigger Cortex Search if the query involves:
Qualitative Language: "Tell me about," "what is," "find information on," "explain," "summarize," "what does the documentation say."
Unstructured Content References: Mentions of "documents," "reports," "presentations," "emails," "articles," "manuals," or a "knowledge base."
The expected answer is a textual explanation, a summary, or a link to a document.
Default Action and Ambiguity Resolution:
If a query is ambiguous or could be answered by either tool, your default action is to start with Cortex Search to gather broad context first.
If the initial search results suggest that a precise, structured answer is available, you may then follow up with a targeted call to Cortex Analyst.
Never guess. If the user's intent remains unclear after your initial analysis, ask a clarifying question like, "Are you looking for a specific number from our database, or for a general explanation from our documents?\"""",
    "messages": [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "Can you show me a breakdown of customer support tickets by service type cellular vs business internet?"
                }
            ]
        }
    ],
    "tools": [
        {
            "tool_spec": {
                "type": "cortex_analyst_text_to_sql",
                "name": "revenue_analyst"
            }
        }
    ],
    "tool_resources": {
        "revenue_analyst": {
            "semantic_view": "CORTEX_ANALYST_DEMO.FAKE_GBO.GBO_MODEL",
            "description": """The GBO_MODEL semantic view provides a comprehensive analysis framework for Avatar movie's business performance across multiple dimensions. It combines domestic box office metrics (weekly revenue, theater counts) with international market performance (country-wise distribution, market share) and integrates supply chain operations (product delivery, supplier management). The view enables analysis of both theatrical performance and physical product distribution, spanning from CORTEX_ANALYST_DEMO database's FAKE_GBO and REVENUE_TIMESERIES schemas. This integrated view allows tracking of the complete business cycle from theatrical release to product merchandising and distribution.

This semantic view combines movie performance metrics (both domestic and international) with supply chain data, suggesting it's designed to analyze the complete business operation of Avatar movie, from box office performance to physical product distribution and sales."""
        }
    }
}

# Send the POST request
headers = {
    "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
    "Authorization": f"Bearer {jwt_token}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

try:
    response = requests.post(
        os.getenv("AGENT_ENDPOINT"),
        headers=headers,
        data=json.dumps(payload)
    )
    response.raise_for_status()
    print("✅ Cortex Agents response:\n\n", response.text)

except requests.exceptions.RequestException as e:
    print("❌ Request error:", str(e))
    if hasattr(e, 'response') and e.response is not None:
        print("Response status code:", e.response.status_code)
        print("Response body:", e.response.text)