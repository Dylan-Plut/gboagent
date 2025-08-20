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
        self.jwt = JWTGenerator(self.account, self.user, self.private_key_path, self.private_key_password).get_token()

    def _retrieve_response(self, query: str, limit=1) -> dict[str, any]:
        url = self.agent_url
        headers = {
            'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f"Bearer {self.jwt}"
        }
        
        # Corrected data payload with proper Snowflake Cortex Agent API format
        data = {
            "model": self.model,
            "response_instruction": """You are a sophisticated and helpful Data Intelligence Assistant. Your primary purpose is to provide users with accurate insights from the company's data ecosystem. 

You have access to the revenue_analyst tool which uses Cortex Analyst for quantitative questions against structured data. This tool excels at performing calculations, aggregations (like sum, count, average), trend analysis, and answering questions about specific business metrics, KPIs, tables, or columns.

IMPORTANT: Always use the revenue_analyst tool to answer data-related questions. Do not provide answers without using the tool first. The tool has access to Avatar GBO (Global Box Office) data and can generate SQL queries to retrieve the requested information.

When a user asks for data, metrics, or analysis, immediately use the revenue_analyst tool to query the semantic model and retrieve the accurate data.""",
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
                    "tool_spec": {
                        "type": "cortex_analyst_text_to_sql",
                        "name": "revenue_analyst"
                    }
                }
            ],
            "tool_resources": {
                "revenue_analyst": {
                    "semantic_view": self.semantic_model
                }
            },
            "tool_choice": {
                "type": "auto"
            },
            "experimental": {}
        }

        if DEBUG:
            print("Request payload:")
            print(json.dumps(data, indent=2))
        
        try:
            response = requests.post(url, headers=headers, json=data, timeout=60)
        except requests.exceptions.Timeout:
            print("Request timed out. The Cortex Agent might be busy or unresponsive.")
            return {"text": "Request timed out. Please try again.", "sql": "", "citations": ""}
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return {"text": f"Request failed: {e}", "sql": "", "citations": ""}

        if response.status_code == 401:  # Unauthorized - likely expired JWT
            print("JWT has expired. Generating new JWT...")
            try:
                # Generate new token
                self.jwt = JWTGenerator(self.account, self.user, self.private_key_path, self.private_key_password).get_token()
                # Retry the request with the new token
                headers["Authorization"] = f"Bearer {self.jwt}"
                print("New JWT generated. Sending new request to Cortex Agents API. Please wait...")
                response = requests.post(url, headers=headers, json=data, timeout=60)
            except Exception as e:
                print(f"Failed to regenerate JWT: {e}")
                return {"text": f"Authentication failed: {e}", "sql": "", "citations": ""}

        if DEBUG:
            print(f"Response status code: {response.status_code}")
            print("Response headers:")
            for key, value in response.headers.items():
                print(f"  {key}: {value}")
            print("Response content:")
            print(response.text[:1000])  # Print first 1000 chars

        if response.status_code == 200:
            return self._parse_response(response)
        else:
            try:
                error_details = response.json()
                print(f"Error: Received status code {response.status_code}")
                print(f"Error details: {json.dumps(error_details, indent=2)}")
                return {"text": f"Error {response.status_code}: {error_details}", "sql": "", "citations": ""}
            except:
                print(f"Error: Received status code {response.status_code} with message {response.text}")
                return {"text": f"Error {response.status_code}: {response.text}", "sql": "", "citations": ""}

    def _parse_delta_content(self, content: list) -> dict[str, any]:
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

    def _process_sse_line(self, line: str) -> dict[str, any]:
        """Process a single SSE line and return parsed content."""
        if not line.startswith('data: '):
            return {}
        try:
            json_str = line[6:].strip()  # Remove 'data: ' prefix
            if json_str == '[DONE]':
                return {'type': 'done'}
                
            data = json.loads(json_str)
            if DEBUG:
                print(f"SSE Data: {json.dumps(data, indent=2)}")
                
            if data.get('object') == 'message.delta':
                delta = data.get('delta', {})
                if 'content' in delta:
                    return {
                        'type': 'message',
                        'content': self._parse_delta_content(delta['content'])
                    }
            return {'type': 'other', 'data': data}
        except json.JSONDecodeError as e:
            if DEBUG:
                print(f"JSON decode error: {e}")
                print(f"Problematic line: {line}")
            return {'type': 'error', 'message': f'Failed to parse: {line}'}
    
    def _parse_response(self, response: requests.Response) -> dict[str, any]:
        """Parse and print the SSE chat response with improved organization."""
        accumulated = {
            'text': '',
            'tool_use': [],
            'tool_results': [],
            'other': []
        }

        try:
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
                    elif result.get('type') == 'error' and DEBUG:
                        print(f"Parse error: {result['message']}")
        except Exception as e:
            print(f"Error parsing response: {e}")
            return {"text": f"Error parsing response: {e}", "sql": "", "citations": ""}

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

        # Enhanced tool result parsing
        if accumulated['tool_results']:
            for result in accumulated['tool_results']:
                if isinstance(result, dict):
                    # Handle different result structures
                    if 'content' in result:
                        for content in result['content']:
                            if isinstance(content, dict):
                                if 'json' in content and isinstance(content['json'], dict):
                                    if 'sql' in content['json']:
                                        sql = content['json']['sql']
                                    if 'citations' in content['json']:
                                        citations = content['json']['citations']
                                elif 'sql' in content:
                                    sql = content['sql']
                    elif 'sql' in result:
                        sql = result['sql']
                    elif 'output' in result and isinstance(result['output'], dict):
                        if 'sql' in result['output']:
                            sql = result['output']['sql']

        # If we have tool usage but no results, indicate that tools were attempted
        if accumulated['tool_use'] and not sql and not citations:
            if DEBUG:
                print("Tools were used but no SQL/results were extracted")
            # Check if there's any indication of tool failure in the text
            if "error" in text.lower() or "unable" in text.lower():
                text += "\n\n(Note: Tool execution may have encountered issues)"

        return {"text": text, "sql": sql, "citations": citations}
       
    def chat(self, query: str) -> any:
        """Main chat interface with enhanced error handling"""
        try:
            response = self._retrieve_response(query)
            if not response:
                return {"text": "I apologize, but I couldn't process your request at this time. Please try again.", "sql": "", "citations": ""}
            return response
        except Exception as e:
            print(f"Error in chat method: {e}")
            return {"text": f"An error occurred while processing your request: {e}", "sql": "", "citations": ""}