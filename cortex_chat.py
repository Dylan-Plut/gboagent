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
            private_key_path: str
        ):
        self.agent_url = agent_url
        self.model = model
        self.semantic_model = semantic_model
        self.account = account
        self.user = user
        self.private_key_path = private_key_path
        self.jwt = JWTGenerator(self.account, self.user, self.private_key_path).get_token()

    def _retrieve_response(self, query: str, limit=1) -> dict[str, any]:
        url = self.agent_url
        headers = {
            'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f"Bearer {self.jwt}"
        }
        data = {
            "model": self.model,
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
                    "semantic_model_file": self.semantic_model,
                    "description": """The GBO_MODEL semantic view provides a comprehensive analysis framework for Avatar movie's business performance across multiple dimensions. It combines domestic box office metrics (weekly revenue, theater counts) with international market performance (country-wise distribution, market share) and integrates supply chain operations (product delivery, supplier management). The view enables analysis of both theatrical performance and physical product distribution, spanning from CORTEX_ANALYST_DEMO database's FAKE_GBO and REVENUE_TIMESERIES schemas. This integrated view allows tracking of the complete business cycle from theatrical release to product merchandising and distribution.

This semantic view combines movie performance metrics (both domestic and international) with supply chain data, suggesting it's designed to analyze the complete business operation of Avatar movie, from box office performance to physical product distribution and sales."""
                }
            },
        }
        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 401:  # Unauthorized - likely expired JWT
            print("JWT has expired. Generating new JWT...")
            # Generate new token
            self.jwt = JWTGenerator(self.account, self.user, self.private_key_path).get_token()
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
                            if 'sql' in content['json']:
                                sql = content['json']['sql']

        return {"text": text, "sql": sql, "citations": citations}
       
    def chat(self, query: str) -> any:
        response = self._retrieve_response(query)
        return response