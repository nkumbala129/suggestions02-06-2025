import streamlit as st
import json
import re
import requests
import snowflake.connector
import pandas as pd
from snowflake.snowpark import Session
from snowflake.core import Root
from typing import Any, Dict, List, Optional, Tuple
import plotly.express as px
import time

# --- Snowflake/Cortex Configuration ---
HOST = "HLGSIYM-COB42429.snowflakecomputing.com"
DATABASE = "AI"
SCHEMA = "DWH_MART"
API_ENDPOINT = "/api/v2/cortex/agent:run"
API_TIMEOUT = 50000  # in milliseconds
CORTEX_SEARCH_SERVICES = "PROC_SERVICE"
SEMANTIC_MODEL = '@"AI"."DWH_MART"."PROCUREMENT_SEARCH"/procurement.yaml'

# --- Model Options ---
MODELS = [
    "mistral-large",
    "snowflake-arctic",
    "llama3-70b",
    "llama3-8b",
]

# --- Streamlit Page Config ---
st.set_page_config(
    page_title="Welcome to Cortex AI Assistant",
    layout="wide",
    initial_sidebar_state="auto"
)

# --- Session State Initialization ---
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.username = ""
    st.session_state.password = ""
    st.session_state.CONN = None
    st.session_state.snowpark_session = None
    st.session_state.chat_history = []
    st.session_state.messages = []
if "last_suggestions" not in st.session_state:
    st.session_state.last_suggestions = []
if "chart_x_axis" not in st.session_state:
    st.session_state.chart_x_axis = None
if "chart_y_axis" not in st.session_state:
    st.session_state.chart_y_axis = None
if "chart_type" not in st.session_state:
    st.session_state.chart_type = "Bar Chart"
if "current_query" not in st.session_state:
    st.session_state.current_query = None
if "current_results" not in st.session_state:
    st.session_state.current_results = None
if "current_sql" not in st.session_state:
    st.session_state.current_sql = None
if "current_summary" not in st.session_state:
    st.session_state.current_summary = None
if "service_metadata" not in st.session_state:
    st.session_state.service_metadata = [{"name": "PROC_SERVICE", "search_column": ""}]
if "selected_cortex_search_service" not in st.session_state:
    st.session_state.selected_cortex_search_service = "PROC_SERVICE"
if "model_name" not in st.session_state:
    st.session_state.model_name = "mistral-large"
if "num_retrieved_chunks" not in st.session_state:
    st.session_state.num_retrieved_chunks = 100
if "num_chat_messages" not in st.session_state:
    st.session_state.num_chat_messages = 10
if "use_chat_history" not in st.session_state:
    st.session_state.use_chat_history = True
if "clear_conversation" not in st.session_state:
    st.session_state.clear_conversation = False

# --- CSS Styling ---
st.markdown("""
<style>
#MainMenu, header, footer {visibility: hidden;}
[data-testid="stChatMessage"] {
    opacity: 1 !important;
    background-color: transparent !important;
    white-space: pre-wrap !important;
    word-wrap: break-word !important;
    overflow: hidden !important;
}
[data-testid="stChatMessageContent"] {
    white-space: pre-wrap !important;
    word-wrap: break-word !important;
    overflow: hidden !important;
    width: 100% !important;
    max-width: 100% !important;
    box-sizing: border-box !important;
}
.copy-button, [data-testid="copy-button"], [title="Copy to clipboard"], [data-testid="stTextArea"] {
    display: none !important;
}
.dilytics-logo {
    position: fixed;
    top: 10px;
    right: 10px;
    z-index: 1000;
    width: 150px;
    height: auto;
}
.fixed-header {
    position: fixed;
    top: 0;
    left: 20px;
    right: 0;
    z-index: 999;
    background-color: #ffffff;
    padding: 10px;
    text-align: center;
    pointer-events: none;
}
.fixed-header a {
    pointer-events: none !important;
    text-decoration: none !important;
    color: inherit !important;
    cursor: default !important;
}
.stApp {
    padding-top: 100px;
}
</style>
""", unsafe_allow_html=True)

# --- Add Logo in the Main UI ---
if st.session_state.authenticated:
    st.markdown(
        f'<img src="https://raw.githubusercontent.com/nkumbala129/30-05-2025/main/Dilytics_logo.png" class="dilytics-logo">',
        unsafe_allow_html=True
    )

# --- Stream Text Function ---
def stream_text(text: str, chunk_size: int = 1, delay: float = 0.01):
    for i in range(0, len(text), chunk_size):
        yield text[i:i + chunk_size]
        time.sleep(delay)

# --- Start New Conversation ---
def start_new_conversation():
    st.session_state.chat_history = []
    st.session_state.messages = []
    st.session_state.current_query = None
    st.session_state.current_results = None
    st.session_state.current_sql = None
    st.session_state.current_summary = None
    st.session_state.chart_x_axis = None
    st.session_state.chart_y_axis = None
    st.session_state.chart_type = "Bar Chart"
    st.session_state.last_suggestions = []
    st.session_state.clear_conversation = False

# --- Initialize Service Metadata ---
@st.cache_data
def init_service_metadata():
    st.session_state.service_metadata = [{"name": "PROC_SERVICE", "search_column": ""}]
    st.session_state.selected_cortex_search_service = "PROC_SERVICE"
    try:
        svc_search_col = session.sql("DESC CORTEX SEARCH SERVICE PROC_SERVICE;").collect()[0]["search_column"]
        st.session_state.service_metadata = [{"name": "PROC_SERVICE", "search_column": svc_search_col}]
    except Exception as e:
        st.error(f"❌ Failed to verify PROC_SERVICE: {str(e)}. Using default configuration.")

# --- Initialize Config Options ---
def init_config_options():
    with st.sidebar.form(key="config_form"):
        st.button("Clear conversation", on_click=start_new_conversation)
        st.toggle("Use chat history", key="use_chat_history", value=True)
        with st.expander("Advanced options"):
            st.selectbox("Select model:", MODELS, key="model_name")
            st.number_input(
                "Select number of context chunks",
                value=100,
                key="num_retrieved_chunks",
                min_value=1,
                max_value=400
            )
            st.number_input(
                "Select number of messages to use in chat history",
                value=10,
                key="num_chat_messages",
                min_value=1,
                max_value=100
            )
        st.form_submit_button("Apply Settings")

# --- Query Cortex Search Service ---
@st.cache_data
def query_cortex_search_service(query):
    try:
        db, schema = session.get_current_database(), session.get_current_schema()
        root = Root(session)
        cortex_search_service = (
            root.databases[db]
            .schemas[schema]
            .cortex_search_services["PROC_SERVICE"]
        )
        context_documents = cortex_search_service.search(
            query, columns=[], limit=st.session_state.num_retrieved_chunks
        )
        results = context_documents.results
        service_metadata = st.session_state.service_metadata
        search_col = service_metadata[0]["search_column"]
        context_str = ""
        for i, r in enumerate(results):
            context_str += f"Context document {i+1}: {r[search_col]} \n" + "\n"
        return context_str
    except Exception as e:
        st.error(f"❌ Error querying Cortex Search service: {str(e)}")
        return ""

# --- Get Chat History ---
def get_chat_history():
    start_index = max(
        0, len(st.session_state.chat_history) - st.session_state.num_chat_messages
    )
    return st.session_state.chat_history[start_index : len(st.session_state.chat_history) - 1]

# --- Make Chat History Summary ---
def make_chat_history_summary(chat_history, question):
    chat_history_str = "\n".join([f"{msg['role']}: {msg['content']}" for msg in chat_history])
    prompt = f"""
        [INST]
        Based on the chat history below and the question, generate a query that extends the question
        with the chat history provided. The query should be in natural language.
        Answer with only the query. Do not add any explanation.

        <chat_history>
        {chat_history_str}
        </chat_history>
        <question>
        {question}
        </question>
        [/INST]
    """
    summary = complete(st.session_state.model_name, prompt)
    return summary

# --- Create Prompt ---
def create_prompt(user_question):
    chat_history_str = ""
    if st.session_state.use_chat_history:
        chat_history = get_chat_history()
        if chat_history:
            question_summary = make_chat_history_summary(chat_history, user_question)
            prompt_context = query_cortex_search_service(question_summary)
            chat_history_str = "\n".join([f"{msg['role']}: {msg['content']}" for msg in chat_history])
        else:
            prompt_context = query_cortex_search_service(user_question)
    else:
        prompt_context = query_cortex_search_service(user_question)
        chat_history = []
    
    if not prompt_context.strip():
        return complete(st.session_state.model_name, user_question)
    
    prompt = f"""
        [INST]
        You are a helpful AI chat assistant with RAG capabilities. When a user asks you a question,
        you will also be given context provided between <context> and </context> tags. Use that context
        with the user's chat history provided in the between <chat_history> and </chat_history> tags
        to provide a summary that addresses the user's question. Ensure the answer is coherent, concise,
        and directly relevant to the user's question.

        If the user asks a generic question which cannot be answered with the given context or chat_history,
        just respond directly and concisely to the user's question using the LLM.

        <chat_history>
        {chat_history_str}
        </chat_history>
        <context>
        {prompt_context}
        </context>
        <question>
        {user_question}
        </question>
        [/INST]
        Answer:
    """
    return complete(st.session_state.model_name, prompt)

# --- Authentication Logic ---
if not st.session_state.authenticated:
    st.title("Welcome to Snowflake Cortex AI")
    st.write("Please login to interact with your data")
    st.session_state.username = st.text_input("Enter Snowflake Username:", value=st.session_state.username)
    st.session_state.password = st.text_input("Enter Password:", type="password")
    if st.button("Login"):
        try:
            conn = snowflake.connector.connect(
                user=st.session_state.username,
                password=st.session_state.password,
                account="HLGSIYM-COB42429",
                host=HOST,
                port=443,
                warehouse="COMPUTE_WH",
                role="ACCOUNTADMIN",
                database=DATABASE,
                schema=SCHEMA,
            )
            st.session_state.CONN = conn
            snowpark_session = Session.builder.configs({
                "connection": conn
            }).create()
            st.session_state.snowpark_session = snowpark_session
            with conn.cursor() as cur:
                cur.execute(f"USE DATABASE {DATABASE}")
                cur.execute(f"USE SCHEMA {SCHEMA}")
                cur.execute("ALTER SESSION SET TIMEZONE = 'UTC'")
                cur.execute("ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE")
            st.session_state.authenticated = True
            st.success("Authentication successful! Redirecting...")
            st.rerun()
        except Exception as e:
            st.error(f"Authentication failed: {e}")
else:
    # --- Main App Logic ---
    session = st.session_state.snowpark_session
    root = Root(session)

    # --- Run Snowflake Query ---
    @st.cache_data
    def run_snowflake_query(query):
        try:
            if not query:
                return None
            df = session.sql(query)
            data = df.collect()
            if not data:
                return None
            columns = df.schema.names
            result_df = pd.DataFrame(data, columns=columns)
            return result_df
        except Exception as e:
            st.error(f"❌ SQL Execution Error: {str(e)}")
            return None

    # --- Query Classification Functions ---
    def is_structured_query(query: str):
        structured_patterns = [
            r'\b(count|number|where|group by|order by|sum|avg|max|min|total|how many|which|show|list|names?|are there any|rejected deliveries?|least|highest|duration|approval)\b',
            r'\b(vendor|supplier|requisition|purchase order|po|organization|department|buyer|delivery|received|billed|rejected|late|on time|late deliveries?|approval duration)\b'
        ]
        return any(re.search(pattern, query.lower()) for pattern in structured_patterns)

    def is_complete_query(query: str):
        complete_patterns = [r'\b(generate|write|create|describe|explain)\b']
        return any(re.search(pattern, query.lower()) for pattern in complete_patterns)

    def is_summarize_query(query: str):
        summarize_patterns = [r'\b(summarize|summary|condense)\b']
        return any(re.search(pattern, query.lower()) for pattern in summarize_patterns)

    def is_question_suggestion_query(query: str):
        suggestion_patterns = [
            r'\b(what|which|how)\b.*\b(questions|type of questions|queries)\b.*\b(ask|can i ask|pose)\b',
            r'\b(give me|show me|list)\b.*\b(questions|examples|sample questions)\b'
        ]
        return any(re.search(pattern, query.lower()) for pattern in suggestion_patterns)

    def is_greeting_query(query: str):
        greeting_patterns = [
            r'^\b(hello|hi|hey|greet)\b$',
            r'^\b(hello|hi|hey|greet)\b\s.*$'
        ]
        return any(re.search(pattern, query.lower()) for pattern in greeting_patterns)

    # --- Cortex Complete Function ---
    def complete(model, prompt):
        try:
            prompt = prompt.replace("'", "\\'")
            query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{prompt}') AS response"
            result = session.sql(query).collect()
            return result[0]["RESPONSE"]
        except Exception as e:
            st.error(f"❌ COMPLETE Function Error: {str(e)}")
            return None

    # --- Summarize Function ---
    def summarize(text):
        try:
            text = text.replace("'", "\\'")
            query = f"SELECT SNOWFLAKE.CORTEX.SUMMARIZE('{text}') AS summary"
            result = session.sql(query).collect()
            return result[0]["SUMMARY"]
        except Exception as e:
            st.error(f"❌ SUMMARIZE Function Error: {str(e)}")
            return None

    # --- Parse SSE Response ---
    def parse_sse_response(response_text: str) -> List[Dict]:
        events = []
        lines = response_text.strip().split("\n")
        current_event = {}
        for line in lines:
            if line.startswith("event:"):
                current_event["event"] = line.split(":", 1)[1].strip()
            elif line.startswith("data:"):
                data_str = line.split(":", 1)[1].strip()
                if data_str != "[DONE]":
                    try:
                        data_json = json.loads(data_str)
                        current_event["data"] = data_json
                        events.append(current_event)
                        current_event = {}
                    except json.JSONDecodeError as e:
                        st.error(f"❌ Failed to parse SSE data: {str(e)} - Data: {data_str}")
        return events

    # --- Process SSE Response ---
    def process_sse_response(response, is_structured):
        sql = ""
        search_results = []
        if not response:
            return sql, search_results
        try:
            for event in response:
                if event.get("event") == "message.delta" and "data" in event:
                    delta = event["data"].get("delta", {})
                    content = delta.get("content", [])
                    for item in content:
                        if item.get("type") == "tool_results":
                            tool_results = item.get("tool_results", {})
                            if "content" in tool_results:
                                for result in tool_results["content"]:
                                    if result.get("type") == "json":
                                        result_data = result.get("json", {})
                                        if is_structured and "sql" in result_data:
                                            sql = result_data.get("sql", "")
                                        elif not is_structured and "searchResults" in result_data:
                                            search_results = [sr["text"] for sr in result_data["searchResults"]]
        except Exception as e:
            st.error(f"❌ Error Processing Response: {str(e)}")
        return sql.strip(), search_results

    # --- Snowflake API Call ---
    def snowflake_api_call(query: str, is_structured: bool = False):
        payload = {
            "model": st.session_state.model_name,
            "messages": [{"role": "user", "content": [{"type": "text", "text": query}]}],
            "tools": []
        }
        if is_structured:
            payload["tools"].append({"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "analyst1"}})
            payload["tool_resources"] = {"analyst1": {"semantic_model_file": SEMANTIC_MODEL}}
        else:
            payload["tools"].append({"tool_spec": {"type": "cortex_search", "name": "search1"}})
            payload["tool_resources"] = {"search1": {"name": "PROC_SERVICE", "max_results": st.session_state.num_retrieved_chunks}}
        try:
            resp = requests.post(
                url=f"https://{HOST}{API_ENDPOINT}",
                json=payload,
                headers={
                    "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
                    "Content-Type": "application/json",
                },
                timeout=API_TIMEOUT // 1000
            )
            if resp.status_code < 400:
                if not resp.text.strip():
                    st.error("❌ API returned an empty response.")
                    return None
                return parse_sse_response(resp.text)
            else:
                raise Exception(f"Failed request with status {resp.status_code}: {resp.text}")
        except Exception as e:
            st.error(f"❌ API Request Error: {str(e)}")
            return None

    # --- Summarize Unstructured Answer ---
    def summarize_unstructured_answer(answer):
        sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|")\s', answer)
        return "\n".join(f"- {sent.strip()}" for sent in sentences[:6])

    # --- Suggest Sample Questions ---
    def suggest_sample_questions(query: str) -> List[str]:
        try:
            prompt = (
                f"The user asked: '{query}'. This question may be ambiguous or unclear in the context of a business-facing procurement analytics assistant. "
                f"Generate 3-5 clear, concise sample questions related to purchase orders, requisitions, suppliers, or procurement metrics. "
                f"The questions should be easy for a business user to understand and answerable using procurement data such as supplier names, requisition statuses, or PO values. "
                f"Format as a numbered list. Example format:\n1. Which suppliers have the highest purchase order amounts?\n2. What is the approval rate for requisitions this quarter?"
            )
            response = complete(st.session_state.model_name, prompt)
            if response:
                questions = []
                for line in response.split("\n"):
                    line = line.strip()
                    if re.match(r'^\d+\.\s*.+', line):
                        question = re.sub(r'^\d+\.\s*', '', line)
                        questions.append(question)
                return questions[:5]
            else:
                return [
                    "Which buyer has the most purchase orders submitted in the last month?",
                    "What is the average time taken for PO approval by each buyer in the current fiscal year?",
                    "Which suppliers have the longest lead times for delivering goods after PO approval?",
                    "What is the total value of purchase orders approved by each department in the last quarter?",
                    "Which requisitions have been pending approval for more than a week?"
                ]
        except Exception as e:
            st.error(f"❌ Failed to generate sample questions: {str(e)}")
            return [
                "Which buyer has the most purchase orders submitted in the last month?",
                "What is the average time taken for PO approval by each buyer in the current fiscal year?",
                "Which suppliers have the longest lead times for delivering goods after PO approval?",
                "What is the total value of purchase orders approved by each department in the last quarter?",
                "Which requisitions have been pending approval for more than a week?"
            ]

    # --- Display Chart Function ---
    def display_chart_tab(df: pd.DataFrame, prefix: str = "chart", query: str = ""):
        if df.empty or len(df.columns) < 2:
            return
        query_lower = query.lower()
        if re.search(r'\b(county|jurisdiction)\b', query_lower):
            default_chart = "Pie Chart"
        elif re.search(r'\b(month|year|date)\b', query_lower):
            default_chart = "Line Chart"
        else:
            default_chart = "Bar Chart"
        all_cols = list(df.columns)
        col1, col2, col3 = st.columns(3)
        default_x = st.session_state.get(f"{prefix}_x", all_cols[0])
        x_index = all_cols.index(default_x) if default_x in all_cols else 0
        x_col = col1.selectbox("X axis", all_cols, index=x_index, key=f"{prefix}_x")
        remaining_cols = [c for c in all_cols if c != x_col]
        default_y = st.session_state.get(f"{prefix}_y", remaining_cols[0] if remaining_cols else all_cols[0])
        y_index = remaining_cols.index(default_y) if default_y in remaining_cols else 0
        y_col = col2.selectbox("Y axis", remaining_cols, index=y_index, key=f"{prefix}_y")
        chart_options = ["Line Chart", "Bar Chart", "Pie Chart", "Scatter Chart", "Histogram Chart"]
        default_type = st.session_state.get(f"{prefix}_type", default_chart)
        type_index = chart_options.index(default_type) if default_type in chart_options else chart_options.index(default_chart)
        chart_type = col3.selectbox("Chart Type", chart_options, index=type_index, key=f"{prefix}_type")
        chart_key = f"{prefix}_{chart_type}_{hash(query)}"
        if chart_type == "Line Chart":
            fig = px.line(df, x=x_col, y=y_col, title=chart_type)
            st.plotly_chart(fig, key=chart_key)
        elif chart_type == "Bar Chart":
            fig = px.bar(df, x=x_col, y=y_col, title=chart_type)
            st.plotly_chart(fig, key=chart_key)
        elif chart_type == "Pie Chart":
            fig = px.pie(df, names=x_col, values=y_col, title=chart_type)
            st.plotly_chart(fig, key=chart_key)
        elif chart_type == "Scatter Chart":
            fig = px.scatter(df, x=x_col, y=y_col, title=chart_type)
            st.plotly_chart(fig, key=chart_key)
        elif chart_type == "Histogram Chart":
            fig = px.histogram(df, x=x_col, title=chart_type)
            st.plotly_chart(fig, key=chart_key)

    # --- Sidebar UI ---
    with st.sidebar:
        st.markdown("""
        <style>
        [data-testid="stSidebar"] [data-testid="stButton"] > button {
            background-color: #29B5E8 !important;
            color: white !important;
            font-weight: bold !important;
            width: 100% !important;
            border-radius: 0px !important;
            margin: 0 !important;
            border: none !important;
            padding: 0.5rem 1rem !important;
        }
        </style>
        """, unsafe_allow_html=True)
        logo_container = st.container()
        button_container = st.container()
        about_container = st.container()
        help_container = st.container()
        with logo_container:
            logo_url = "https://www.snowflake.com/wp-content/themes/snowflake/assets/img/logo-blue.svg"
            st.image(logo_url, width=250)
        with button_container:
            init_config_options()
        with about_container:
            st.markdown("### About")
            st.write(
                "This application uses **Snowflake Cortex Analyst** to interpret "
                "your natural language questions and generate data insights. "
                "Simply ask a question below to see relevant answers and visualizations."
            )
        with help_container:
            st.markdown("### Help & Documentation")
            st.write(
                "- [User Guide](https://docs.snowflake.com/en/guides-overview-ai-features)  \n"
                "- [Snowflake Cortex Analyst Docs](https://docs.snowflake.com/)  \n"
                "- [Contact Support](https://www.snowflake.com/en/support/)"
            )

    # --- Main UI and Query Processing ---
    with st.container():
        st.markdown(
            """
            <div class="fixed-header">
                <h1 style='color: #29B5E8; margin-bottom: 5px;'>   Cortex AI-Procurement Assistant by DiLytics</h1>
                <p style='font-size: 16px; color: #333;'><strong>Welcome to Cortex AI. I am here to help with Dilytics Procurement Insights Solutions</strong></p>
            </div>
            """,
            unsafe_allow_html=True
        )
    semantic_model_filename = SEMANTIC_MODEL.split("/")[-1]
    init_service_metadata()

    st.sidebar.subheader("Sample Questions")
    sample_questions = [
        "What is DiLytics Procurement Insight Solution?",
        "What are the key subject areas covered in the solution?",
        "Describe the key metrics tracked in the Purchase Requisition reports.",
        "Show total purchase order value by organization.",
        "Which supplier has the highest requisition amount?",
        "How many active purchase orders are there?",
        "Which supplier has the minimum and maximum PO delivery rate?",
        "Which buyer has the least and highest PO approval duration?",
        "What are the top 5 suppliers based on purchase order amount?"
    ]

    if st.session_state.chat_history:
        for message in st.session_state.chat_history[-10:]:
            with st.chat_message(message["role"]):
                st.write(message["content"])
                if message["role"] == "assistant" and "results" in message and message["results"] is not None:
                    with st.expander("View SQL Query", expanded=False):
                        st.code(message["sql"], language="sql")
                    st.write(f"Query Results ({len(message['results'])} rows):")
                    st.dataframe(message["results"], key=f"df_{hash(message['content'])}")
                    if not message["results"].empty and len(message["results"].columns) >= 2:
                        st.write("Visualization:")
                        display_chart_tab(message["results"], prefix=f"chart_{hash(message['content'])}", query=message.get("query", ""))

    query = st.chat_input("Ask your question...")
    if query and query.lower().startswith("no of"):
        query = query.replace("no of", "number of", 1)
    for sample in sample_questions:
        if st.sidebar.button(sample, key=f"sample_{sample}"):
            query = sample

    if query:
        st.session_state.chart_x_axis = None
        st.session_state.chart_y_axis = None
        st.session_state.chart_type = "Bar Chart"
        original_query = query
        selected_question = None
        if query.strip().isdigit() and st.session_state.last_suggestions:
            try:
                index = int(query.strip()) - 1
                if 0 <= index < len(st.session_state.last_suggestions):
                    selected_question = st.session_state.last_suggestions[index]
                    query = selected_question
                else:
                    st.warning(f"Invalid selection: {query}. Please choose a number between 1 and {len(st.session_state.last_suggestions)}.")
                    query = original_query
            except Exception:
                query = original_query
        st.session_state.chat_history.append({"role": "user", "content": original_query})
        st.session_state.messages.append({"role": "user", "content": original_query})
        with st.chat_message("user"):
            st.write(original_query)
        with st.chat_message("assistant"):
            with st.spinner("Generating Response..."):
                is_structured = is_structured_query(query)
                is_complete = is_complete_query(query)
                is_summarize = is_summarize_query(query)
                is_suggestion = is_question_suggestion_query(query)
                is_greeting = is_greeting_query(query)
                assistant_response = {"role": "assistant", "content": "", "query": query}
                response_content = ""
                failed_response = False

                if is_greeting or is_suggestion:
                    greeting = original_query.lower().split()[0]
                    if greeting not in ["hi", "hello", "hey", "greet"]:
                        greeting = "Hello"
                    response_content = f"{greeting}! I'm here to help with your procurement analytics questions. Here are some questions you can ask me:\n\n"
                    selected_questions = sample_questions[:5]
                    for i, q in enumerate(selected_questions, 1):
                        response_content += f"{i}. {q}\n"
                    response_content += "\nFeel free to ask any of these or come up with your own related to procurement analytics!"
                    st.write_stream(stream_text(response_content))
                    assistant_response["content"] = response_content
                    st.session_state.last_suggestions = selected_questions
                    st.session_state.messages.append({"role": "assistant", "content": response_content})

                elif is_complete:
                    response = create_prompt(query)
                    if response:
                        response_content = response.strip()
                        st.write_stream(stream_text(response_content))
                        assistant_response["content"] = response_content
                        st.session_state.messages.append({"role": "assistant", "content": response_content})
                    else:
                        response_content = ""
                        failed_response = True
                        assistant_response["content"] = response_content

                elif is_summarize:
                    summary = summarize(query)
                    if summary:
                        response_content = summary.strip()
                        st.write_stream(stream_text(response_content))
                        assistant_response["content"] = response_content
                        st.session_state.messages.append({"role": "assistant", "content": response_content})
                    else:
                        response_content = ""
                        failed_response = True
                        assistant_response["content"] = response_content

                elif is_structured:
                    response = snowflake_api_call(query, is_structured=True)
                    sql, _ = process_sse_response(response, is_structured=True)
                    if sql:
                        results = run_snowflake_query(sql)
                        if results is not None and not results.empty:
                            results_text = results.to_string(index=False)
                            prompt = f"Provide a concise natural language answer to the query '{query}' using the following data, avoiding phrases like 'Based on the query results':\n\n{results_text}"
                            summary = complete(st.session_state.model_name, prompt)
                            if not summary:
                                summary = "Unable to generate a natural language summary."
                            response_content = summary.strip()
                            st.write_stream(stream_text(response_content))
                            with st.expander("View SQL Query", expanded=False):
                                st.code(sql, language="sql")
                            st.write(f"Query Results ({len(results)} rows):")
                            st.dataframe(results, key=f"df_{hash(query)}")
                            if len(results.columns) >= 2:
                                st.write("Visualization:")
                                display_chart_tab(results, prefix=f"chart_{hash(query)}", query=query)
                            assistant_response.update({
                                "content": response_content,
                                "sql": sql,
                                "results": results,
                                "summary": summary
                            })
                            st.session_state.messages.append({
                                "role": "assistant",
                                "content": response_content,
                                "sql": sql,
                                "results": results,
                                "summary": summary
                            })
                        else:
                            response_content = "No data returned for the query."
                            failed_response = True
                            assistant_response["content"] = response_content
                    else:
                        response_content = "Failed to generate SQL query."
                        failed_response = True
                        assistant_response["content"] = response_content

                else:
                    response = snowflake_api_call(query, is_structured=False)
                    _, search_results = process_sse_response(response, is_structured=False)
                    if search_results:
                        raw_result = search_results[0]
                        summary = create_prompt(query)
                        if summary:
                            response_content = summary.strip()
                            st.write_stream(stream_text(response_content))
                        else:
                            response_content = summarize_unstructured_answer(raw_result).strip()
                            st.write_stream(stream_text(response_content))
                        assistant_response["content"] = response_content
                        st.session_state.messages.append({"role": "assistant", "content": response_content})
                    else:
                        response_content = ""
                        failed_response = True
                        assistant_response["content"] = response_content

                if failed_response:
                    suggestions = suggest_sample_questions(query)
                    response_content = "I'm not sure about your question. Here are some questions you can ask me:\n\n"
                    for i, suggestion in enumerate(suggestions, 1):
                        response_content += f"{i}. {suggestion}\n"
                    response_content += "\nThese questions might help clarify your query. Feel free to try one or rephrase your question!"
                    st.write_stream(stream_text(response_content))
                    assistant_response["content"] = response_content
                    st.session_state.last_suggestions = suggestions
                    st.session_state.messages.append({"role": "assistant", "content": response_content})

                st.session_state.chat_history.append(assistant_response)
                st.session_state.current_query = query
                st.session_state.current_results = assistant_response.get("results")
                st.session_state.current_sql = assistant_response.get("sql")
                st.session_state.current_summary = assistant_response.get("summary")
