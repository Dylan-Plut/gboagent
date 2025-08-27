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

    def chat(self, query: str, conn, callback=None) -> dict:
        print(f"--- Received query: {query} ---")
        self.history = [{"role": "user", "content": [{"type": "text", "text": query}]}]
        
        # First API call to get SQL and interpretation
        print("--- Sending first API call to get SQL ---")
        
        # Initial callback to show we're processing
        if callback:
            callback("I'm analyzing your question...")
        
        response_one = self._send_request()
        if response_one.status_code != 200:
            error_msg = f"API Error on first call: Status {response_one.status_code}"
            print(f"--- {error_msg} ---")
            if callback:
                callback(error=error_msg)
            return {"error": error_msg}

        # Stream the first response to the user as we receive it
        assistant_parts_one = []
        current_text = ""
        
        for line in response_one.iter_lines():
            if not line: continue
            decoded_line = line.decode('utf-8')
            if not decoded_line.startswith('data: '): continue
            try:
                json_str = decoded_line[6:].strip()
                if json_str == '[DONE]': break
                data = json.loads(json_str)
                if isinstance(data, dict) and data.get('object') == 'message.delta':
                    delta_content = data.get('delta', {}).get('content', [])
                    if isinstance(delta_content, list):
                        for part in delta_content:
                            assistant_parts_one.append(part)
                            # If text content, update the callback
                            if part.get('type') == 'text' and callback:
                                current_text += part.get('text', '')
                                callback(current_text)
            except json.JSONDecodeError:
                print(f"Warning: Failed to parse SSE line: {decoded_line}")
    
        print(f"--- First API response parts: {json.dumps(assistant_parts_one, indent=2)} ---")
        
        if not assistant_parts_one: 
            error_msg = "Agent returned an empty response on first call."
            print(f"--- {error_msg} ---")
            if callback:
                callback(error=error_msg)
            return {"error": error_msg}
        
        # Extract text from regular text parts
        initial_interpretation = "".join(part.get('text', '') for part in assistant_parts_one if part.get('type') == 'text')
        print(f"--- Initial text interpretation: {initial_interpretation} ---")
        
        # Extract interpretation from tool results
        self.history.append({"role": "assistant", "content": assistant_parts_one})
        sql_results_part = next((part for part in assistant_parts_one if part.get('type') == 'tool_results'), None)
        
        tool_interpretation = ""
        if sql_results_part:
            # Extract the interpretation from the tool results json
            tool_results = sql_results_part.get('tool_results', {})
            content = tool_results.get('content', [{}])
            if content and len(content) > 0:
                json_content = content[0].get('json', {})
                tool_interpretation = json_content.get('text', '')
                print(f"--- Tool interpretation: {tool_interpretation} ---")
        
        # Use the tool interpretation if available, otherwise fall back to initial text
        final_interpretation = tool_interpretation if tool_interpretation else initial_interpretation
        print(f"--- Final interpretation to use: {final_interpretation} ---")
        
        if not sql_results_part:
            print("--- No SQL generated, returning interpretation ---")
            if callback:
                callback(final_interpretation, is_final=True)
            return {"text": final_interpretation or "I couldn't interpret your request", "dataframe": None, "sql": None}

        # Execute SQL
        tool_results = sql_results_part.get('tool_results', {})
        sql_query = tool_results.get('content', [{}])[0].get('json', {}).get('sql')
        print(f"--- Tool results structure: {json.dumps(tool_results, indent=2)} ---")

        if not isinstance(sql_query, str):
            error_msg = "Agent did not provide a valid SQL query."
            print(f"--- {error_msg}: {sql_query} ---")
            if callback:
                callback(error=error_msg, sql=str(sql_query))
            return {"error": error_msg, "sql": str(sql_query)}
        
        # Update the user that we're executing SQL
        if callback:
            callback(f"{final_interpretation}\n\n_Executing SQL query..._")
    
        print(f"--- Executing SQL: {sql_query} ---")
        try:
            df = pd.read_sql(sql_query, conn)
            print(f"--- SQL execution successful. Rows: {len(df)}, Columns: {list(df.columns)} ---")
        except Exception as e:
            error_msg = str(e)
            print(f"--- SQL execution error: {error_msg} ---")
            if callback:
                callback(error=error_msg, sql=sql_query)
            return {"error": error_msg, "sql": sql_query}

        # Send data back for summary
        if callback:
            callback(f"{final_interpretation}\n\n_Processing results..._")
        
        print("--- Sending second API call for summary ---")
        tool_data = {"type": "text", "text": df.to_json(orient='records')}
        self.history.append({"role": "user", "content": [{"type": "tool_results", "tool_results": {"tool_name": tool_results.get('tool_name'), "content": [tool_data]}}]})

        # Second API call to get summary
        response_two = self._send_request()
        if response_two.status_code != 200:
            print(f"--- Error on second API call: {response_two.status_code} ---")
            # Return tool interpretation when second call fails
            if callback:
                callback(final_interpretation, is_final=True, df=df, sql=sql_query)
            return {
                "text": final_interpretation,
                "dataframe": df,
                "sql": sql_query,
                "warning": f"API Error on second call: Status {response_two.status_code}"
            }

        # Stream the second response 
        assistant_parts_two = []
        current_text = final_interpretation
        
        for line in response_two.iter_lines():
            if not line: continue
            decoded_line = line.decode('utf-8')
            if not decoded_line.startswith('data: '): continue
            try:
                json_str = decoded_line[6:].strip()
                if json_str == '[DONE]': break
                data = json.loads(json_str)
                if isinstance(data, dict) and data.get('object') == 'message.delta':
                    delta_content = data.get('delta', {}).get('content', [])
                    if isinstance(delta_content, list):
                        for part in delta_content:
                            assistant_parts_two.append(part)
                            # If text content, update the callback
                            if part.get('type') == 'text' and callback:
                                current_text = final_interpretation + "\n\n" + part.get('text', '')
                                callback(current_text)
            except json.JSONDecodeError:
                print(f"Warning: Failed to parse SSE line: {decoded_line}")
    
        print(f"--- Second API response parts: {json.dumps(assistant_parts_two, indent=2)} ---")
        
        # If the second response is empty, use the tool interpretation
        if not assistant_parts_two:
            print("--- Empty response from second API call, using tool interpretation ---")
            if callback:
                callback(final_interpretation, is_final=True, df=df, sql=sql_query)
            return {
                "text": final_interpretation,
                "dataframe": df,
                "sql": sql_query,
                "warning": "Summarization failed"
            }

        self.history.append({"role": "assistant", "content": assistant_parts_two})
        final_text = "".join(part.get('text', '') for part in assistant_parts_two if part.get('type') == 'text')
        print(f"--- Final summary text: {final_text} ---")
        
        # If the agent returns text but it's empty, use tool interpretation
        if not final_text.strip():
            print("--- Empty summary text, using tool interpretation ---")
            if callback:
                callback(final_interpretation, is_final=True, df=df, sql=sql_query)
            return {
                "text": final_interpretation,
                "dataframe": df,
                "sql": sql_query,
                "warning": "Empty summary"
            }

        # Final callback with complete results
        if callback:
            complete_text = final_text if final_text.strip() else final_interpretation
            callback(complete_text, is_final=True, df=df, sql=sql_query)

        return {"text": final_text, "dataframe": df, "sql": sql_query}

