"""ZTM AI Dispatcher

Streamlit chat app that wraps a LangChain SQL agent + Google Gemini LLM
to answer questions about the ZTM (Warsaw public transport) "Gold" DB.

Features:
- Loads human-readable table/column metadata from `table_metadata.json`.
- Builds a SQLDatabase tool and a tool-calling agent to run SQL safely.
- Presents a chat UI and augments user queries with current Warsaw time.
"""

import streamlit as st
import os
import json
from langchain_community.utilities import SQLDatabase
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_community.agent_toolkits import SQLDatabaseToolkit, create_sql_agent
from datetime import datetime
from zoneinfo import ZoneInfo

# --- Database connection settings (change for your environment) ---
DB_HOST = "postgres"
DB_NAME = "Warsaw_Bus_DB"
DB_USER = "admin"
DB_PASSWORD = "admin"
DB_PORT = 5432

# Gemini API key (expected as an environment variable)
api_key = os.getenv("GEMINI_API_KEY")

# Configure Streamlit page appearance
st.set_page_config(page_title="ZTM Data Agent", page_icon="🚌", layout="centered")

# Initialize chat history in the Streamlit session state (keeps state between reruns)
if "messages" not in st.session_state:
    st.session_state.messages = [{
        "role": "assistant",
        "content": (
            "Hello! What would you like to know about our ZTM database? "
            "You can ask e.g. about which districts are currently the most delayed."
        ),
    }]

st.title("ZTM AI Dispatcher")
st.markdown("")



@st.cache_resource
def setup_agent(api_key):
    """Construct and cache the SQL agent.

    The agent is created once and cached by Streamlit to avoid repeated
    cold-start costs. This function:
    - Builds a SQLAlchemy URI for the Postgres Gold schema.
    - Loads `table_metadata.json` to provide rich table/column descriptions.
    - Wraps the DB in LangChain's `SQLDatabase` and creates a toolkit.
    - Instantiates the Google Gemini LLM adapter and the SQL agent.
    """
    # SQLAlchemy-compatible URI used by langchain-community's SQLDatabase
    db_uri = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    # Load table/column metadata shipped with the app for better prompts
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(current_dir, "table_metadata.json")
    
    with open(json_path, "r", encoding="utf-8") as file:
        raw_metadata = json.load(file)

    # Format readable descriptions per table to pass into the SQL tool
    custom_descriptions = {}
    for table_name, table_data in raw_metadata.items():
        compiled_text = f"{table_data['description']}\nKolumny w tej tabeli:\n"
        for col_name, col_desc in table_data['columns'].items():
            compiled_text += f"- {col_name}: {col_desc}\n"
        custom_descriptions[table_name] = compiled_text

    # Create the SQLDatabase wrapper so the agent can introspect and query
    db = SQLDatabase.from_uri(
        db_uri,
        schema="gold",
        include_tables=list(custom_descriptions.keys()),
        custom_table_info=custom_descriptions,
    )

    # Initialize the LLM (Gemini via the provided adapter)
    llm = ChatGoogleGenerativeAI(temperature=0, model="gemini-3-flash", google_api_key=api_key)

    # Create a toolkit that exposes the DB as tools the agent can call
    toolkit = SQLDatabaseToolkit(db=db, llm=llm)

    # The prefix guides the agent's behavior, timezone handling and output format
    custom_prefix = """You are an expert data analyst for ZTM Warsaw public transport. You have access to a PostgreSQL database (GOLD layer).
    Always analyze the available tables and their business descriptions first.
    IMPORTANT: Use Markdown tables if you are displaying multiple results.
    IMPORTANT: Remeber to use warsaw time (UTC+2) when analyzing time-based data and always convert it to UTC before querying the database, as the database stores all timestamps in UTC.
    IMPORTANT: When presenting results, convert timestamps back to Warsaw time (UTC+2).
    IMPORTANT: Never try to output hundreds of rows. If a query returns many results, aggregate them or LIMIT the SQL query to the top 10 most relevant records."""

    # Build the tool-calling SQL agent and return it
    agent_executor = create_sql_agent(
        llm=llm,
        toolkit=toolkit,
        verbose=True,
        agent_type="tool-calling",
        prefix=custom_prefix,
    )
    return agent_executor


try:
    # Initialize the cached agent; show a UI error if setup fails
    agent = setup_agent(api_key)
except Exception as e:
    st.error(f"Error setting up agent: {e}")
    st.stop()


# Render existing conversation messages
for msg in st.session_state.messages:
    st.chat_message(msg["role"]).write(msg["content"])


# Main chat input handler: collect user input, augment with Warsaw time,
# send to the agent and display the response.
if user_query := st.chat_input("Write your question here..."):
    # Persist and display the user's message
    st.session_state.messages.append({"role": "user", "content": user_query})
    st.chat_message("user").write(user_query)
    
    # Compute current Warsaw time and include it in the prompt so the agent
    # can interpret relative expressions like "today" or "now" correctly.
    warsaw_time = datetime.now(ZoneInfo("Europe/Warsaw")).strftime("%Y-%m-%d %H:%M:%S")

    augmented_query = f"""
    Question: {user_query}
    
    [CRITICAL POST-PROCESSING INSTRUCTION]
    The current date and time in Warsaw is: {warsaw_time}. 
    """

    with st.chat_message("assistant"):
        with st.spinner("Searching the Gold layer... "):
            try:
                # Ask the agent to process the augmented query and return results
                result = agent.invoke({"input": augmented_query})
                raw_output = result.get("output") if isinstance(result, dict) else result["output"]
                
                # Normalize potential output shapes from the agent
                if isinstance(raw_output, list) and len(raw_output) > 0 and isinstance(raw_output[0], dict) and "text" in raw_output[0]:
                    clean_response = raw_output[0]["text"]
                elif isinstance(raw_output, dict) and "text" in raw_output:
                    clean_response = raw_output["text"]
                else:
                    clean_response = str(raw_output)
                
                # Display and persist the assistant response
                st.write(clean_response)

                st.session_state.messages.append({"role": "assistant", "content": clean_response})
                
            except Exception as e:
                # Surface runtime errors from the agent to the user
                st.error(f"Agent encountered a problem while executing the query: {e}")