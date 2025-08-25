import requests
import json
import generate_jwt
from generate_jwt import JWTGenerator

DEBUG = False

class CortexChat:
    def __init__(self,
            agent_url: str,
            semantic_model: str,
            model: str,
            account: str,
            user: str,
            private_key_path: str,
            private_key_password: str = None
        ):
        self.agent_url = agent_url
        self.model = model
        self.semantic_model = semantic_model
        self.account = account
        self.user = user
        self.private_key_path = private_key_path
        self.private_key_password = private_key_password
        self.jwt_generator = JWTGenerator(self.account, self.user, self.private_key_path, self.private_key_password)
        self.jwt = self.jwt_generator.get_token()

    def _retrieve_response(self, query: str, limit=1) -> dict[str, any]:
        url = self.agent_url
        headers = {
            'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f"Bearer {self.jwt}"
        }

        # --- START OF FIX: Correct payload structure based on dev example ---
        data = {
            "model": self.model,
            "response_instruction":""""
            You are a sophisticated and helpful Data Intelligence Assistant. Your primary purpose is to provide users with accurate insights from the company's data ecosystem. You have access to one distinct, powerful tool to accomplish this:
            Cortex Analyst: Use this tool for quantitative questions against structured data. It excels at performing calculations, aggregations (like sum, count, average), trend analysis, and answering questions about specific business metrics, KPIs, tables, or columns.

            REASONING:
            This semantic view combines movie performance metrics (both domestic and international) with supply chain data, suggesting it's designed to analyze the complete business operation of Avatar movie, from box office performance to physical product distribution and sales.

            DESCRIPTION:
            The GBO_MODEL semantic view provides a comprehensive analysis framework for Avatar movie's business performance across multiple dimensions. It combines domestic box office metrics (weekly revenue, theater counts) with international market performance (country-wise distribution, market share) and integrates supply chain operations (product delivery, supplier management). The view enables analysis of both theatrical performance and physical product distribution, spanning from CORTEX_ANALYST_DEMO database's FAKE_GBO and REVENUE_TIMESERIES schemas. This integrated view allows tracking of the complete business cycle from theatrical release to product merchandising and distribution.

            AVATAR_DOMESTIC:
            - Database: CORTEX_ANALYST_DEMO, Schema: FAKE_GBO
            - Tracks domestic box office performance metrics for Avatar movie
            - Contains weekly performance indicators and theater statistics
            - LIST OF COLUMNS: DATE (showing date), AVG (average revenue per theater), CUM_GROSS (cumulative gross revenue), GROSS (weekly gross revenue), G_CHANGE (percentage change in gross), RANK (box office ranking), THEATERS (number of theaters showing), TICKETS (number of tickets sold), T_CHANGE (change in theater count), WEEK_NO (week number of release)

            AVATAR_FOREIGN:
            - Database: CORTEX_ANALYST_DEMO, Schema: FAKE_GBO
            - Captures international market performance for Avatar movie
            - Provides regional distribution and market share analysis
            - LIST OF COLUMNS: COUNTRY (release country), DISTRIBUTOR (distribution company), GROSS_SHARE (percentage of total gross), REGION (geographical region), GROSS (total gross revenue), OPENING (opening weekend revenue), OPEN_SHARE (opening revenue percentage), POPULATION (country population), RELEASE_DATE (movie release date)

            SUPPLY_CHAIN:
            - Database: CORTEX_ANALYST_DEMO, Schema: REVENUE_TIMESERIES
            - Manages product delivery and supplier information
            - Tracks shipping metrics and product performance
            - LIST OF COLUMNS: INVOICE_NUMBER (unique invoice identifier), PAYMENT_TERMS (payment conditions), PRODUCT_ID (unique product identifier), PRODUCT_NAME (name of product), SHIPPING_START_LOCATION (origin point), SUPPLIER_VENDOR_NAME (supplier name), AVERAGE_PRODUCT_PRICE (mean product cost), AVERAGE_SHIPPING_TIME (mean delivery duration), CAPACITY (storage/shipping capacity), DELIVERY_TIME (actual delivery duration), PRICE (product price), RETURN_RATE (product return percentage), DELIVERY_DATE (final delivery date), ORDER_DATE (purchase date), SHIP_DATE (shipping initiation date)
            """,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": query
                        }
                    ]
                }
            ],
            "tools": [
                {
                    # FIX 1: Re-introduced the "tool_spec" wrapper as required by the API.
                    "tool_spec": {
                        "type": "cortex_analyst_text_to_sql",
                        "name": "supply_chain"
                    }
                }
            ],
            "tool_resources": {
                "supply_chain": {
                    # FIX 2: Using "semantic_model_file" as the key, consistent with the dev code.
                    "semantic_model_file": self.semantic_model
                }
            }
        }
        # --- END OF FIX ---

        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 401:  # Unauthorized - likely expired JWT
            print("JWT has expired. Generating new JWT...")
            # Generate new token
            self.jwt = self.jwt_generator.get_token()
            # Retry the request with the new token
            headers["Authorization"] = f"Bearer {self.jwt}"
            print("New JWT generated. Sending new request to Cortex Agents API. Please wait...")
            response = requests.post(url, headers=headers, json=data)

        if DEBUG:
            print(response.text)
        if response.status_code == 200:
            return self._parse_response(response)
        else:
            print(f"Error: Received status code {response.status_code} with message {response.json()}")
            return None

    def _parse_delta_content(self,content: list) -> dict[str, any]:
        """Parse different types of content from the delta."""
        result = {
            'text': '',
            'tool_use': [],
            'tool_results': []
        }

        for entry in content:
            entry_type = entry.get('type')
            if entry_type == 'text':
                result['text'] += entry.get('text', '')
            elif entry_type == 'tool_use':
                result['tool_use'].append(entry.get('tool_use', {}))
            elif entry_type == 'tool_results':
                result['tool_results'].append(entry.get('tool_results', {}))

        return result

    def _process_sse_line(self,line: str) -> dict[str, any]:
        """Process a single SSE line and return parsed content."""
        if not line.startswith('data: '):
            return {}
        try:
            json_str = line[6:].strip()  # Remove 'data: ' prefix
            if json_str == '[DONE]':
                return {'type': 'done'}

            data = json.loads(json_str)
            if data.get('object') == 'message.delta':
                delta = data.get('delta', {})
                if 'content' in delta:
                    return {
                        'type': 'message',
                        'content': self._parse_delta_content(delta['content'])
                    }
            return {'type': 'other', 'data': data}
        except json.JSONDecodeError:
            return {'type': 'error', 'message': f'Failed to parse: {line}'}

    def _parse_response(self,response: requests.Response) -> dict[str, any]:
        """Parse and print the SSE chat response with improved organization."""
        accumulated = {
            'text': '',
            'tool_use': [],
            'tool_results': [],
            'other': []
        }

        for line in response.iter_lines():
            if line:
                result = self._process_sse_line(line.decode('utf-8'))

                if result.get('type') == 'message':
                    content = result['content']
                    accumulated['text'] += content['text']
                    accumulated['tool_use'].extend(content['tool_use'])
                    accumulated['tool_results'].extend(content['tool_results'])
                elif result.get('type') == 'other':
                    accumulated['other'].append(result['data'])

        text = ''
        sql = ''
        citations = ''

        if accumulated['text']:
            text = accumulated['text']

        if DEBUG:
            print("\n=== Complete Response ===")

            print("\n--- Generated Text ---")
            print(text)

            if accumulated['tool_use']:
                print("\n--- Tool Usage ---")
                print(json.dumps(accumulated['tool_use'], indent=2))

            if accumulated['other']:
                print("\n--- Other Messages ---")
                print(json.dumps(accumulated['other'], indent=2))

            if accumulated['tool_results']:
                print("\n--- Tool Results ---")
                print(json.dumps(accumulated['tool_results'], indent=2))

        if accumulated['tool_results']:
            for result in accumulated['tool_results']:
                for k,v in result.items():
                    if k == 'content':
                        for content in v:
                            if 'json' in content and 'sql' in content['json']:
                                sql = content['json']['sql']

        return {"text": text, "sql": sql, "citations": citations}

    def chat(self, query: str) -> any:
        response = self._retrieve_response(query)
        return response