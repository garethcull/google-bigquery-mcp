# Google BigQuery MCP Server

A Model Context Protocol (MCP) server that exposes Google BigQuery datasets, schemas, and SQL execution tools to MCP Clients such as OpenAI Responses API, Cursor, or the prototypr.ai MCP Client.

This MCP Server is implemented as a Flask application and can run locally, on Google Cloud Run, or on your own infrastructure.

# BigQuery MCP Tools

This BigQuery MCP Server provides four MCP tools to help you explore BigQuery:

1. get_list_of_datasets_by_project_id
2. get_table_schema
3. create_custom_sql_query_to_review
4. run_sql_query

These tools allow natural‑language exploration of a BigQuery project: listing datasets, inspecting table schemas, generating SQL queries with LLM assistance, and executing queries once reviewed and approved by the user.

This server requires authorization via a shared token (MCP_TOKEN). All requests to the /mcp endpoint must include a Bearer token in the Authorization header.

# Set Up Your Environment

### Create a Virtual Environment

Create a virtual environment using standard Python venv or Anaconda Navigator.

#### Using Anaconda Navigator

I really like using Anaconda Navigator for managing virtual environments. It's easy to use and setup as follows:

1. Open Anaconda Navigator
2 Select Environments
3. Create a new environment (Python 3.11 recommended)
4. Activate it before running this project

This provides an isolated environment for dependencies and prevents package conflicts.

### Install dependencies

Within your newly setup environment, open the folder where bigquery-mcp lives and pip install the required modules.

```python
pip install -r requirements.txt
```

### Update Environment Variables

The following environment variables must be configured:

1. GCP_BQ_BASE64_KEY - This is a base64‑encoded Google Cloud service account key. Users must encode their JSON key file as base64 before running the MCP server. You can find this referenced in mcp_helper.py.
2. GOOGLE_AI_KEY - This is your Gemini API key, which is used to call Gemini 2.5 Flash for SQL generation via natural language. You can find this referenced in mcp_helper.py.
3. MCP_TOKEN - Required for authenticating MCP requests. You will need to manually set this value and then use this as part of an auth header in your mcp client to connect to the server. You can find this referenced in app.py.

There is also a manual bigquery project id that you need to manually set. This doesn't necessarily need to be an environment variable. 
project_id = "your-bigquery-project-id"


### Running the server locally and how to test it.

In your terminal, navigate to the folder you downloaded this mcp server to. Then run the following command:

```python
flask run --debugger --reload -h localhost -p 3000
```

If you would like to hit the mcp endpoints during testing, you can use this:

```python
import requests, json

BASE = "https://<your-cloud-run-host>/mcp"
AUTH = "Bearer <your-mcp-token>"

def rpc(method, params, id_):
    payload = {"jsonrpc":"2.0","id":id_, "method":method, "params":params}
    r = requests.post(BASE, headers={"Authorization": AUTH, "Content-Type":"application/json"}, data=json.dumps(payload))
    print(method, r.status_code)
    print(r.text[:600])
    return r

rpc("initialize", {}, "1")
rpc("tools/list", {}, "2")
rpc("tools/call", {
    "name":"get_list_of_datasets_by_project_id",
    "arguments":{"query":"List all datasets"}
}, "3")
```

# Configuring your MCP Client to talk to the BigQuery MCP Server

This MCP server was originally designed to work with the prototypr.ai MCP Client. If you would like to try this client, you will need a Plus membership plan. 

In prototypr.ai, navigate to your AI Workspace, then click on MCP Tools in the main chat box, then click on the add server button. Here you can simply drop in your MCP Server settings as json and click add server. 

Here's what those credentials look like - you just need to add the MCP_TOKEN so that the mcp client can connect to the BigQuery MCP Server.

```python
{
  "mcpServers": {
    "bigquery-mcp": {
      "url": "https://www.yourdomain.ai/mcp",
      "displayName": "Google BigQuery MCP",
      "description": "A mcp server that helps people explore their BigQuery data with natural language",
      "icon": "https://www.yourdomain.ai/bq_icon.png",
      "headers": {
        "Authorization": "Bearer MCP_TOKEN."
      },
      "transport": "stdio"
    }
  }
}
```

Alternatively, you could also try connecting with this BigQuery MCP Server using OpenAI's MCP Client by adding the following tools to a Responses API request:

```python
tools = [
  {
    "type": "mcp",
    "server_label": "bigquery-mcp",
    "server_url": "https://<your-cloud-run-host>/mcp",
    "headers": { "Authorization": "Bearer <MCP_TOKEN>" },
    "require_approval": "never"
  }
]
```

For more details about OpenAI's Responses API and MCP, please check out this cookbook: 
https://cookbook.openai.com/examples/mcp/mcp_tool_guide

# About this MCP Architecture

This MCP server contains two files:
1. app.py - main python file which authenticates and delegates requests to mcp_helper.py
2. mcp_helper.py - supporting helper functions to fulfill user requests.

### app.py
* Flask app with POST /mcp
* Handles JSON-RPC notifications by returning 204 No Content
* Delegates to mcp_helper for MCP method logic

### mcp_helper.py
* handle_request routes initialize, tools/list, tools/call
* handle_tool_call decodes arguments, dispatches to tools, and returns MCP-shaped results

### Endpoints and Protocol
* JSON-RPC MCP (preferred by this server)
* POST /mcp
* Content-Type: application/json
* Auth: Authorization: Bearer MCP_TOKEN

### Methods
* initialize → returns protocolVersion, serverInfo, capabilities
* tools/list → returns tools with inputSchema (camelCase)
* tools/call → returns result with content array
* notifications/initialized → must NOT return a JSON-RPC body; respond 204

# Adding New Tools


Adding new tools is easy. Just add a new object with the name of the tool corresponding to a the same named function and include any parameters that you would pass into these functions. 

```python
def handle_tools_list():
    return {
        "tools": [
            {
                "name": "get_list_of_datasets_by_project_id",
                "description": "Describe this tool",
                "annotations": {"read_only": False},
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"}
                    },
                    "required": ["query"],
                    "additionalProperties": False
                }
            }
        ]
    }
```

# Security Considerations

* Always require Authorization: Bearer MCP_TOKEN on /mcp
* Keep tool outputs reasonable in size and fully UTF‑8

# Deploying to Google Cloud Run

This MCP server was originally designed to be deployed on Google Cloud Run. 

Google Cloud Run is a serverless environment where you can host mcp applications such as this Flask based one. 

Important Note: Google Cloud Run is a paid service, so you'll need to ensure your project is set and billing enabled. 

You will also need to add the Environment Variables to your mcp project instance.

This article was extremely helpful for me and teaches you how to deploy a flask application like this this MCP Server to Google Cloud Run:

https://docs.cloud.google.com/run/docs/quickstarts/build-and-deploy/deploy-python-service

# License
MIT (or your preferred license).

# Contributions & Support
Feedback, issues and PRs welcome. Due to bandwidth constraints, I can't offer any timelines for free updates to this codebase. 

If you need help customizing this MCP server or bringing your BigQuery data together to take advantage of this awesome server, I'm available for paid consulting and freelance projects. 

Please feel free to reach out and connect w/ me on LinkedIn:
https://www.linkedin.com/in/garethcull/

Thanks for checking out this Google BigQuery MCP Server! I hope it helps you and your team.

Happy Querying!
