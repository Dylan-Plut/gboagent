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
    "preamble": """""",
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