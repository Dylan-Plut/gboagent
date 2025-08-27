import requests
import json
import pandas as pd
from generate_jwt import JWTGenerator

class CortexChat:
    def __init__(self, agent_url: str, model: str, account: str, user: str, private_key_path: str,
                 tools: list, tool_resources: dict, response_instruction: str = "You are a helpful assistant.",
                 private_key_password: str = None):
        self.agent_url = agent_url
        self.model = model
        self.response_instruction = response_instruction
        self.tools = tools
        self.tool_resources = tool_resources
        self.jwt_generator = JWTGenerator(account, user, private_key_path, private_key_password)
        self.jwt = self.jwt_generator.get_token()
        self.history = []

    def _send_request(self) -> requests.Response:
        headers = {'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT', 'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': f"Bearer {self.jwt}"}
        data = {"model": self.model, "response_instruction": self.response_instruction, "messages": self.history, "tools": self.tools, "tool_resources": self.tool_resources}
        response = requests.post(self.agent_url, headers=headers, json=data, stream=True)
        if response.status_code == 401:
            self.jwt = self.jwt_generator.get_token()
            headers['Authorization'] = f"Bearer {self.jwt}"
            response = requests.post(self.agent_url, headers=headers, json=data, stream=True)
        return response

    def _parse_sse_stream(self, response: requests.Response) -> list:
        assistant_content_parts = []
        for line in response.iter_lines():
            if not line: continue
            decoded_line = line.decode('utf-8')
            if not decoded_line.startswith('data: '): continue
            try:
                json_str = decoded_line[6:].strip()
                if json_str == '[DONE]': break
                data = json.loads(json_str)
                if isinstance(data, dict) and data.get('object') == 'message.delta':
                    delta_content = data.get('delta', {}).get('content', [])
                    if isinstance(delta_content, list): assistant_content_parts.extend(delta_content)
            except json.JSONDecodeError:
                print(f"Warning: Failed to parse SSE line: {decoded_line}")
        return assistant_content_parts

    def chat(self, query: str, conn) -> dict:
        self.history = [{"role": "user", "content": [{"type": "text", "text": query}]}]
        
        # First API call to get SQL
        response_one = self._send_request()
        if response_one.status_code != 200:
            return {"error": f"API Error on first call: Status {response_one.status_code}"}

        assistant_parts_one = self._parse_sse_stream(response_one)
        if not assistant_parts_one: return {"error": "Agent returned an empty response on first call."}
        
        self.history.append({"role": "assistant", "content": assistant_parts_one})
        sql_results_part = next((part for part in assistant_parts_one if part.get('type') == 'tool_results'), None)
        
        if not sql_results_part:
            text = "".join(part.get('text', '') for part in assistant_parts_one if part.get('type') == 'text')
            return {"text": text, "dataframe": None, "sql": None}

        # Execute SQL
        tool_results = sql_results_part.get('tool_results', {})
        sql_query = tool_results.get('content', [{}])[0].get('json', {}).get('sql')

        if not isinstance(sql_query, str):
            return {"error": "Agent did not provide a valid SQL query.", "sql": str(sql_query)}
        
        print(f"--- Extracted SQL: {sql_query} ---")
        try:
            df = pd.read_sql(sql_query, conn)
        except Exception as e:
            return {"error": str(e), "sql": sql_query}

        # Send data back for summary
        tool_data = {"type": "text", "text": df.to_json(orient='records')}
        self.history.append({"role": "user", "content": [{"type": "tool_results", "tool_results": {"tool_name": tool_results.get('tool_name'), "content": [tool_data]}}]})

        # Second API call to get summary
        response_two = self._send_request()
        if response_two.status_code != 200:
            return {"error": f"API Error on second call: Status {response_two.status_code}", "sql": sql_query, "dataframe": df}

        assistant_parts_two = self._parse_sse_stream(response_two)
        
        # --- THE DEFINITIVE FIX TO THE LOGIC IS HERE ---
        # If the second response is empty, it's not a total failure. It's a partial success.
        if not assistant_parts_two:
            print("--- Agent returned an empty summary. Returning data with a warning. ---")
            return {
                "text": "I successfully retrieved the data for you, but I encountered an issue while generating a summary. Here is the raw data:",
                "dataframe": df,
                "sql": sql_query,
                "warning": "Summarization failed." # Add a warning flag
            }
        # --- END OF FIX ---

        self.history.append({"role": "assistant", "content": assistant_parts_two})
        final_text = "".join(part.get('text', '') for part in assistant_parts_two if part.get('type') == 'text')
        
        # If the agent returns text but it's empty, handle it as a partial success too.
        if not final_text.strip():
             return {
                "text": "I successfully retrieved the data for you, but the agent did not provide a summary. Here is the raw data:",
                "dataframe": df,
                "sql": sql_query,
                "warning": "Empty summary."
            }

        return {"text": final_text, "dataframe": df, "sql": sql_query}