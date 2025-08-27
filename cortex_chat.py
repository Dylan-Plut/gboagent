import requests
import json
import pandas as pd
from generate_jwt import JWTGenerator

class CortexChat:
    """
    Manages the full conversational lifecycle with the Snowflake Cortex Agents API,
    with robust logging and correct handling of the 'cortex_analyst_text_to_sql' tool flow.
    """
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
        # History is now managed within the chat method to ensure clean sessions
        self.history = []

    def _send_request(self) -> requests.Response:
        """Sends the current conversation history to the Cortex Agent API."""
        headers = {
            'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f"Bearer {self.jwt}"
        }
        data = {"model": self.model, "response_instruction": self.response_instruction, "messages": self.history, "tools": self.tools, "tool_resources": self.tool_resources}
        response = requests.post(self.agent_url, headers=headers, json=data, stream=True)
        if response.status_code == 401:
            print("JWT may have expired. Retrying with a new token...")
            self.jwt = self.jwt_generator.get_token()
            headers['Authorization'] = f"Bearer {self.jwt}"
            response = requests.post(self.agent_url, headers=headers, json=data, stream=True)
        return response

    def _parse_sse_stream(self, response: requests.Response) -> list:
        """
        Accumulates and returns the complete content from an SSE stream.
        Includes extensive logging to print the raw stream for debugging.
        """
        print("\n--- BEGINNING SSE STREAM PARSE ---")
        assistant_content_parts = []
        for line in response.iter_lines():
            if not line: continue
            decoded_line = line.decode('utf-8')
            
            # --- DETAILED LOGGING ADDED AS REQUESTED ---
            print(f"[RAW SSE]: {decoded_line}")
            
            if not decoded_line.startswith('data: '): continue
            try:
                json_str = decoded_line[6:].strip()
                if json_str == '[DONE]': break
                data = json.loads(json_str)
                if isinstance(data, dict) and data.get('object') == 'message.delta':
                    delta_content = data.get('delta', {}).get('content', [])
                    if isinstance(delta_content, list):
                        assistant_content_parts.extend(delta_content)
            except json.JSONDecodeError:
                print(f"Warning: Failed to parse SSE line: {decoded_line}")
        print("--- FINISHED SSE STREAM PARSE ---\n")
        return assistant_content_parts

    def chat(self, query: str, conn) -> dict:
        """
        Handles a full conversation turn, correctly processing the two-stage tool use.
        """
        # 1. Start the conversation with the user's query
        self.history = [{"role": "user", "content": [{"type": "text", "text": query}]}]
        
        # --- FIRST API CALL: Generate SQL ---
        print("--- Making First API Call (User Query -> Agent) ---")
        response_one = self._send_request()
        if response_one.status_code != 200:
            return {"error": f"API Error on first call: Status {response_one.status_code}"}

        assistant_parts_one = self._parse_sse_stream(response_one)
        if not assistant_parts_one: return {"error": "Agent returned an empty response on first call."}
        
        self.history.append({"role": "assistant", "content": assistant_parts_one})
        
        # 2. Check for service-generated SQL in the first response
        sql_results_part = next((part for part in assistant_parts_one if part.get('type') == 'tool_results'), None)
        
        # If no SQL is found, it's a direct answer.
        if not sql_results_part:
            final_text = "".join(part.get('text', '') for part in assistant_parts_one if part.get('type') == 'text')
            return {"text": final_text, "dataframe": None, "sql": None}

        # 3. Execute the extracted SQL
        tool_results = sql_results_part.get('tool_results', {})
        tool_name = tool_results.get('tool_name')
        results_content = tool_results.get('content', [{}])[0]
        sql_query = results_content.get('json', {}).get('sql')

        if not isinstance(sql_query, str) or not sql_query.strip():
            return {"error": "Agent did not provide a valid SQL query.", "sql": str(sql_query)}
        
        print(f"--- Correctly extracted SQL: {sql_query} ---")
        try:
            df = pd.read_sql(sql_query, conn)
        except Exception as e:
            print(f"--- ERROR executing SQL: {e} ---")
            return {"error": str(e), "sql": sql_query}

        # 4. Send the data back to the agent for summarization
        self.history.append({"role": "user", "content": [{"type": "tool_results", "tool_results": {"tool_name": tool_name, "content": [df.to_json(orient='records')]}}]})

        # --- SECOND API CALL: Generate Summary ---
        print("--- Making Second API Call (Data -> Agent) ---")
        response_two = self._send_request()
        if response_two.status_code != 200:
            return {"error": f"API Error on second call: Status {response_two.status_code}", "sql": sql_query}
        
        assistant_parts_two = self._parse_sse_stream(response_two)
        if not assistant_parts_two:
            # THIS IS THE FAILURE POINT WE WERE HITTING
            print("--- FAILURE: Agent returned an empty response after being sent data. ---")
            return {"error": "Agent failed to summarize the data.", "dataframe": df, "sql": sql_query}

        self.history.append({"role": "assistant", "content": assistant_parts_two})
        
        # 5. Extract the final text summary from the second response
        final_text = "".join(part.get('text', '') for part in assistant_parts_two if part.get('type') == 'text')

        return {"text": final_text, "dataframe": df, "sql": sql_query}