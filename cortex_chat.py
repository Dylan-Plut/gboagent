import requests
import json
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
            #"response_instruction":"",
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