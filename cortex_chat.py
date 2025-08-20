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
        
        # This payload is now based on the official Snowflake documentation.
        data = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": "You are a specialized Data Analyst Assistant. Your only function is to answer quantitative questions by using the provided `gbo_analyst` tool. You must evaluate every user query to see if it can be answered by the tool. If it can, you must use the tool. Do not answer from general knowledge. If the query is not a quantitative question, state that you can only answer data-related questions."
                },
                {
                    "role": "user",
                    "content": [{"type": "text", "text": query}]
                }
            ],
            "tools": [
                {
                    "tool_spec": {
                        "type": "cortex_analyst_text_to_sql",
                        "name": "gbo_analyst"
                    }
                }
            ],
            "tool_resources": {
                "gbo_analyst": {
                    "semantic_model_file": self.semantic_model
                }
            },
        }
        
        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 401:
            print("JWT has expired. Generating new JWT...")
            self.jwt = self.jwt_generator.get_token()
            headers["Authorization"] = f"Bearer {self.jwt}"
            print("New JWT generated. Retrying request...")
            response = requests.post(url, headers=headers, json=data)

        if DEBUG:
            print(response.text)
        if response.status_code == 200:
            return self._parse_response(response)
        else:
            print(f"Error: Received status code {response.status_code} with message {response.json()}")
            return None

    def _parse_delta_content(self,content: list) -> dict[str, any]:
        result = {'text': '', 'tool_use': [], 'tool_results': []}
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
        if not line.startswith('data: '):
            return {}
        try:
            json_str = line[6:].strip()
            if json_str == '[DONE]':
                return {'type': 'done'}
            data = json.loads(json_str)
            if data.get('object') == 'message.delta':
                delta = data.get('delta', {})
                if 'content' in delta:
                    return {'type': 'message', 'content': self._parse_delta_content(delta['content'])}
            return {'type': 'other', 'data': data}
        except json.JSONDecodeError:
            return {'type': 'error', 'message': f'Failed to parse: {line}'}
    
    def _parse_response(self,response: requests.Response) -> dict[str, any]:
        accumulated = {'text': '', 'tool_use': [], 'tool_results': [], 'other': []}
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

        text, sql, citations = '', '', ''
        if accumulated['text']:
            text = accumulated['text']

        if accumulated['tool_results']:
            for result in accumulated['tool_results']:
                if 'content' in result:
                    for content_part in result['content']:
                        if 'json' in content_part and 'sql' in content_part['json']:
                            sql = content_part['json']['sql']
        
        final_answer = ""
        if accumulated['text']:
            final_answer = accumulated['text']
        
        return {"text": final_answer, "sql": sql, "citations": citations}
       
    def chat(self, query: str) -> any:
        response = self._retrieve_response(query)
        return response