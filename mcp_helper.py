# Install Modules
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import GoogleAPICallError
from google.oauth2 import service_account
import pandas as pd
import datetime
from decimal import Decimal
import numpy as np
import json
import uuid
import os
import base64
import requests

# =============================================================================
# Helper Functions
# =============================================================================

def decode_key(base64_key):
    
    # Get the Base64-encoded string from the environment variable
    encoded_key = base64_key      

    if not encoded_key:
        raise ValueError(f"Environment variable '{encoded_key}' not found or is empty.")

    # Decode the Base64 string into JSON
    decoded_bytes = base64.b64decode(encoded_key)
    key_info = json.loads(decoded_bytes.decode('utf-8'))
    
    return key_info


# =============================================================================
# Variables
# =============================================================================

# Google BigQuery Settings
gcp_key_base64 = os.environ.get('GCP_BQ_BASE64_KEY')
project_id = "INSERT_YOUR_GOOGLE_CLOUD_PROJECT_ID"

# Decode base64
key_info = decode_key(gcp_key_base64)

# Build credentials using the decoded key
credentials = service_account.Credentials.from_service_account_info(key_info)

# Create BQ Client
client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id
)

# =============================================================================
# MCP Protocol Request Routing
# =============================================================================

def handle_request(method, params):
    """
    Main request router for MCP (Model Context Protocol) JSON-RPC methods.
    Supported:
      - initialize
      - tools/list
      - tools/call
    Notifications (notifications/*) are handled in app.py (204 No Content).
    """
    if method == "initialize":
        return handle_initialize()
    elif method == "tools/list":
        return handle_tools_list()
    elif method == "tools/call":
        return handle_tool_call(params)
    else:
        # Let app.py wrap unknown methods into a proper JSON-RPC error
        raise ValueError(f"Method not found: {method}")


# =============================================================================
# MCP Protocol Handlers
# =============================================================================

def handle_initialize():
    """
    JSON-RPC initialize response.
    Keep protocolVersion consistent with your current implementation.
    """
    return {
        "protocolVersion": "2024-11-05",
        "serverInfo": {
            "name": "bigquery-mcp",
            "version": "0.1.0"
        },
        "capabilities": {
            "tools": {
                "list": True,
                "call": True
            }
        }
    }


def handle_tools_list():
    """
    JSON-RPC tools/list result.
    IMPORTANT: For JSON-RPC MCP, schema field is camelCase: inputSchema
    """
    return {
        "tools": [
            {
                "name": "get_list_of_datasets_by_project_id",
                "description": f"This tool returns a list of available projects in the following project_id: {project_id}.",
                "annotations": {"read_only": False},
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "The user's question to get the list of datasets by project ID"}
                    },
                    "required": ["query"],
                    "additionalProperties": False
                }
            },
            {
                "name": "get_table_schema",
                "description": "This tool returns the schema for a specific BigQuery table.",
                "annotations": {"read_only": False},
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "table_id": {"type": "string", "description": "The ID of the table to get the schema for. e.g. 'project_id.dataset_id.table_id'"}
                    },
                    "required": ["table_id"],
                    "additionalProperties": False
                }
            },
            {
                "name": "create_custom_sql_query_to_review",
                "description": "Create a custom SQL query for review based on a natural language question and a table ID for the user's review. This must be completed prior to any execution of the query.",
                "annotations": {"read_only": False},
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "question": {
                            "type": "string",
                            "description": "Natural language question to generate a SQL query for."
                        },
                        "table_id": {
                            "type": "string",
                            "description": f"The ID of the table to generate a SQL query for. The complete table that is being queryed must include 3 parts. e.g. '{project_id}.dataset_id.table_id'"
                        }
                    },
                    "required": ["question", "table_id"],
                    "additionalProperties": False
                }
            },
            {
                "name": "run_sql_query",
                "description": "Run the user's requested SQL query.",
                "annotations": {"read_only": False},
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "sql_query": {
                            "type": "string",
                            "description": "The complete sql query with no modifications to be run on the user's BigQuery instance."
                        },
                        "confirmation_key": {
                            "type": "string",
                            "description": f"The key required for an LLM to run the query'"
                        }
                    },
                    "required": ["sql_query", "confirmation_key"],
                    "additionalProperties": False
                }
            }
        ]
    }



def handle_tool_call(params):
    tool_name = params.get("name")
    arguments = params.get("arguments", {})

    # Decode string args if needed
    if isinstance(arguments, str):
        try:
            arguments = json.loads(arguments)
        except Exception:
            return {
                "isError": True,
                "content": [{"type": "text", "text": "Invalid arguments: expected object or JSON string."}]
            }

    if tool_name == "get_list_of_datasets_by_project_id":
        data = get_list_of_datasets_by_project_id(arguments)
        return {"content": [{"type": "text", "text": str(data)}]}

    elif tool_name == "get_table_schema":
        data = get_table_schema(arguments)
        return {"content": [{"type": "text", "text": str(data)}]}

    elif tool_name == "create_custom_sql_query_to_review":
        data = create_custom_sql_query_to_review(arguments)
        return {"content": [{"type": "text", "text": str(data)}]}

    elif tool_name == "run_sql_query":
        data = run_sql_query_via_bq_api(arguments)
        return {"content": [{"type": "text", "text": str(data)}]}

    else:
        return {
            "isError": True,
            "content": [{"type": "text", "text": f"Tool not found: {tool_name}"}]
        }




# =============================================================================
# BigQuery Tool: get_list_of_datasets_by_project_id
# =============================================================================


def get_list_of_datasets_by_project_id(arguments):
    
    # We will accumulate lines of text in this list
    output_lines = []
    
    output_lines.append(f"--- Inspecting Project: {project_id} ---")

    try:
        # 1. Get all datasets
        datasets = list(client.list_datasets(project=project_id))

        if not datasets:
            return "No datasets found in this project."

        for dataset in datasets:
            # Add formatted dataset line
            output_lines.append(f"\n📂 Dataset: {dataset.dataset_id}")

            try:
                # 2. Get all tables in this dataset
                tables = list(client.list_tables(dataset))
                
                if tables:
                    for table in tables:
                        # Add formatted table line
                        output_lines.append(f"   └── 📄 Table: {table.table_id}")
                else:
                    output_lines.append("   └── (No tables in this dataset)")

            except Exception as e:
                output_lines.append(f"   └── [Error listing tables: {e}]")

    except Exception as e:
        return f"Error accessing project '{project_id}': {e}"

    joined_data = "\n".join(output_lines)

    # Join all lines with a newline character to create one big string
    instructions = f"""This following datasets live in the user's Google BigQuery Account:
    {project_id}

    Here are the datasets:
    {joined_data}

    Important: It is important that you reference this project_id ({project_id}) when you return this data. This project_id helps confirm that you are pulling the right information.
    """
    
    return instructions


# =============================================================================
# BigQuery Tool: get_table_schema
# =============================================================================


def get_table_schema(arguments):
    """
    Fetches the schema for a specific BigQuery table and returns it
    as a list of dictionaries.

    Args:        
        table_id (string): ID of the table to get the schema for e.g. "project_id.dataset_id.table_id"

    Returns:
        schema_list (list): List of dictionaries representing the schema
    """

    # Extract table_id from arguments
    table_id = arguments.get('table_id')

    # 1. Get the table object (this makes an API call)
    table = client.get_table(table_id)

    # 2. Access the schema
    # table.schema is a list of SchemaField objects.
    # We use .to_api_repr() to convert them to standard dictionaries.
    schema_list = [field.to_api_repr() for field in table.schema]
    
    return schema_list




# =============================================================================
# BigQuery Tool: create_custom_sql_query_to_review
# =============================================================================


def create_custom_sql_query_to_review(arguments):
    
    """
    Creates a custom SQL query from natural language.
    """

    # Extract question from arguments
    question = arguments.get("question")
    table_id = arguments.get("table_id")
    schema = get_table_schema({"table_id": table_id})

    # You generate SQL using a controlled prompt with examples.
    sql = generate_sql_query(question, schema, table_id)

    return sql


def check_query_validity_and_cost(client, query_string):

    """
    Checks to see if the query is valid and how much the query costs to run
    """

    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

    try:
        query_job = client.query(query_string, job_config=job_config)
       
        bytes_processed = query_job.total_bytes_processed
        terabytes = bytes_processed / (1024 ** 4)
        price_per_tb = 6.25 # Updated to current standard pricing
        estimated_cost = terabytes * price_per_tb

        return {
            "status": "VALID",
            "message": "This query is valid.",
            "estimated_cost_usd": round(estimated_cost, 6),
            "bytes_processed": bytes_processed
        }

    # Catching GoogleAPICallError handles BadRequest, Forbidden, NotFound, etc.
    except GoogleAPICallError as e:
        return {
            "status": "INVALID",
            "message": "The query cannot be executed.",
            "error_details": e.message
        }
    except Exception as e:
        # Optional: Catch non-API errors (like Python coding errors)
        return {
            "status": "ERROR",
            "message": "An unexpected system error occurred.",
            "error_details": str(e)
        }



def generate_sql_query(question, table_schema, table_id):
    """
    Generate a SQL query from natural language using the Gemini API.
    
    Args:
        question (string): The user's natural language question
        table_schema (list): The schema of the table
        table_id (string): The ID of the table

    Returns:
        sql (string): The generated SQL query
    """

    # Validate required parameters
    if not question or not table_id:
        raise ValueError("question and table_id are required")
    
    # Get Gemini API key from environment variable
    gemini_api_key = os.getenv('GOOGLE_AI_KEY')
    if not gemini_api_key:
        raise ValueError("GOOGLE_AI_KEY environment variable is not set")
    
    # Prepare the system prompt using RPG framework
    system_prompt = bq_sql_gnerator_system_prompt(question, table_schema, table_id)

    ai_model = "gemini-2.5-flash"
    
    # Prepare the request payload for Gemini API
    payload = {
        "system_instruction": {
                "parts": [
                    {
                        "text": system_prompt
                    }
                ]
        },
        "contents": [            
            {
                "role": "user",
                "parts": [
                    {
                        "text": f"{question}"
                    }
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0.1,
            "topK": 40,
            "topP": 0.95,
            "maxOutputTokens": 4096,
        }
    }
    
    # Make the API request to Gemini
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{ai_model}:generateContent?key={gemini_api_key}"
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        
        # Extract the generated text from the response
        if 'candidates' in result and len(result['candidates']) > 0:
            
            # The generated SQL query
            generated_sql_query = result['candidates'][0]['content']['parts'][0]['text']

            query_status = check_query_validity_and_cost(client, generated_sql_query)

            if query_status['status'] == 'INVALID':

                print('invalid')

                generated_sql_with_instructions = f"""This SQL query is not valid: 
                {generate_sql_query}

                Important: Guidance for presenting this sql query.

                When a sql query is invalid, you can attempt to correct this query based on the user's question:
                {question}

                This query that was generated from this table has the following schema:
                {table_schema}

                It's also recommended to let the user know that the first pass at generating the query failed and you are requesting user input to validate the query.
                 """

            else:                

                # Clean api query to ensure no additional markdown
                generated_sql_with_instructions = f"""This SQL statement helps pull data from the following Google BigQuery table:
                {table_id}. 

                It helps answer the following question: 
                {question}

                This table has the following schema:
                {table_schema}

                Important: Guidance for presenting this query to the user

                When presenting this query to the user, please wrap the query in pre tags with a class="sql-query" to ensure readability. 
                It's also very important to allow the user to test the query, by adding a <button class="execute-sql-query">Execute SQL Query</button> at the very bottom of your response as a CTA. 
                There is already javascript on the page listening for clicks on this button. 

                Here is the generated sql query:
                {generated_sql_query}   

                Important: Sql Validation
                This sql statement has been validated with the Google BigQuery QueryJobConfig method and can be executed. It is important to communicate to the user how much running this query could cost with the amounts processed for transparency reasons. 
                
                These estimates should be share with the user and are from a dry run validation:
                Estimated cost to run (USD): {query_status['estimated_cost_usd']}
                Estimated bytes processed on run: {query_status['bytes_processed']}                
                """
            
            return {                
                "sql_query": generated_sql_with_instructions,
                "user_query": question,                
                "status": "success"
            }
        else:
            return {
                "sql_query": "No results generated from Gemini API",
                "user_query": question,                
                "status": "no_results"
            }
            
    except requests.exceptions.RequestException as e:
        raise Exception(f"Gemini API request failed: {str(e)}")
    except Exception as e:
        raise Exception(f"Error processing Gemini API response: {str(e)}")


def bq_sql_gnerator_system_prompt(user_question, schema, table_id):

    """
    Background: This function returns the system prompt for generating the appropriate search console api query
    
    Args:
        user_question (string): The user's natural language question
        schema (list): The schema of the table
        table_id (string): The ID of the table

    Returns:
        system_prompt (string): The system prompt for generating the appropriate BigQuery SQL query
    """

    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")    

    schema_text = "\n".join(
        [f"- {col['name']} ({col['type']}, {col['mode']})" for col in schema]
    )
    
    system_prompt = f"""You are an expert BigQuery SQL analyst. You write correct, optimized,
SELECT-only SQL queries based strictly on the table schema and the user's request.
You never guess nonexistent columns. You avoid harmful commands such as
DELETE, UPDATE, INSERT, DROP, TRUNCATE, and CREATE.

# P (Problem):

The user wants a SQL query constructed from a natural-language question.
Today's timestamp is: {current_time}.
You must rely ONLY on the provided schema and table ID.
Schema:
{schema_text}

Table ID: {table_id}

GUIDANCE:
- Always produce a single valid SQL SELECT statement.
- Always wrap the table that is being selected in `` (eg. FROM `project.dataset.table`)
- All table names must include a project_id.datset_id.table_id. 
- Use fully qualified column names only if necessary.
- Add WHERE filters based on the user's question.
- If the question is ambiguous, choose the safest, simplest
  interpretation that returns meaningful results.
- Never include ORDER BY unless explicitly requested.
- Never hallucinate fields that do not appear in the schema.
- Never wrap your SQL in markdown or code fences. Return raw SQL ONLY.

EXAMPLE INPUT QUESTION:
"Show me total impressions by device for yesterday for the site 'https://example.com'"

EXAMPLE SCHEMA:
- data_date (DATE, NULLABLE)
- site_url (STRING, NULLABLE)
- query (STRING, NULLABLE)
- is_anonymized_query (BOOLEAN, NULLABLE)
- country (STRING, NULLABLE)
- search_type (STRING, NULLABLE)
- device (STRING, NULLABLE)
- impressions (INTEGER, NULLABLE)
- clicks (INTEGER, NULLABLE)
- sum_top_position (INTEGER, NULLABLE)

EXAMPLE SQL QUERY:
SELECT
  device,
  SUM(impressions) AS total_impressions
FROM `project.dataset.table`
WHERE site_url = 'https://example.com'
  AND data_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY device;

END OF EXAMPLE.

Now produce the SQL SELECT query for this user question:
\"\"\"{user_question}\"\"\"

Final note:
The current time for this request is: {current_time}
"""

    return system_prompt



# =============================================================================
# BigQuery Tool: run_sql_query
# =============================================================================

def json_serializable_converter(obj):
    """
    Helper function to convert BigQuery types (Datetime, Decimal) 
    into JSON-compatible types.
    """
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj) # Warning: float conversion can lose precision
    return str(obj)

def run_sql_query_via_bq_api(arguments):
    """
    Runs a BigQuery query, returns a pandas DataFrame,
    and outputs a simplified table string.
    """

    sql_query = arguments.get('sql_query')

    # Run query
    query_job = client.query(sql_query)

    # Convert rows to clean Python dicts
    rows = [dict(row) for row in query_job]

    # Ensure all values are JSON-serializable
    json_str = json.dumps(rows, default=json_serializable_converter)
    clean_rows = json.loads(json_str)

    # Convert to DataFrame
    df = pd.DataFrame(clean_rows)

    # Create a simple table string
    table_str = df.to_string(index=False)

    final_results = f"""This data comes from BigQuery. It was pulled using the following query:
    {sql_query}

    When presenting this information to the user, please present as a table if no other data visualizations were requested.
    
    It's important to always show the query underneath the final data so that the user can easily understand how this data was pulled.

    Here is the data:
    {table_str}    
    """

    return table_str




