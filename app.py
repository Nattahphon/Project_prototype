import logging
import re
import traceback
from deep_translator import GoogleTranslator
from dotenv import load_dotenv
import streamlit as st
import os
from datetime import datetime
import pytz
import json
import uuid
import shutil
from typhoon_SPagent_modify import TyphoonAgent, PLOT_DIR
from H_datahandle_app import DataHandler
import matplotlib.pyplot as plt
import numpy as np

load_dotenv()

# Constants and Configurations
APP_NAME = "Data Analysis Assistant 📊"
BASE_SESSION_DIR = "sessions"
TEMP_UPLOAD_DIR = "temp_uploads"

# Set page configuration
st.set_page_config(
    page_title=APP_NAME,
    page_icon="🐘",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    /* Main container */
    .main {
        background-color: #343541;
        padding-bottom: 80px;
    }
    /* User messages */
    .user-message {
        background-color: #40414f;
        color: #d1d5db;
        padding: 15px 20px;
        margin: 20px 0;
        border-radius: 8px;
        max-width: 85%;
        align-self: flex-end;
    }
    /* Assistant messages */
    .assistant-message {
        background-color: #2d2d3a;
        color: #d1d5db;
        padding: 15px 20px;
        margin: 35px 0;
        border-radius: 8px;
        max-width: 85%;
        align-self: flex-start;
    }
    /* Session info */
    .session-info {
        background-color: #2e7d32;  /* Green color */
        color: white;  /* Text color to white for better contrast */
        padding: 10px;
        border-radius: 5px;
        margin: 5px 0;
    }
    /* Input container */
    .fixed-input-container {
        position: fixed;
        bottom: 0;
        margin-top: 20px;
        width: 100%;
        background-color: #343541;
        padding: 10px;
        border-top: 1px solid #565869;
    }
    /* Input field */
    .stTextInput input {
        background-color: #40414f;
        color: white;
        border: 1px solid #565869;
        border-radius: 5px;
        width: 100%;
        padding: 10px;
    }
    /* Buttons */
    .stButton button {
        background-color: #565869;
        color: white;
        border: none;
        padding: 8px 16px;
        border-radius: 5px;
        transition: background-color 0.3s;
    }
    .stButton button:hover {
        background-color: #6e7087;
    }
    /* Plot container */
    .plot-container {
        width: 100%;
        max-width: 800px;
        margin: 20px auto;
        padding: 10px;
        background-color: #2d2d3a;
        border-radius: 8px;
    }
    /* Sidebar customization */
    .sidebar .sidebar-content {
        background-color: #262730;
    }
    /* Status messages */
    .success-message {
        background-color: #1a472a;
        color: #2ecc71;
        padding: 10px;
        border-radius: 5px;
        margin: 5px 0;
    }
    .warning-message {
        background-color: #7d4a00;
        color: #f39c12;
        padding: 10px;
        border-radius: 5px;
        margin: 5px 0;
    }
    .error-message {
        background-color: #700000;
        color: #e74c3c;
        padding: 10px;
        border-radius: 5px;
        margin: 5px 0;
    }
</style>
""", unsafe_allow_html=True)

def convert_json_to_str(data):    
    clean_str = "\n".join([
            f"{key.capitalize()}: {value if isinstance(value, str) else ', '.join(value)}"
            for key, value in data.items()
        ])
    return clean_str

def translate_func(target_lang, text):
    try:
        translated = GoogleTranslator(source='auto', target=target_lang).translate(text)
        return translated
    except:
        return GoogleTranslator(source='auto', target=target_lang).translate(convert_json_to_str(data=text))

def get_model_base_url(model):
    if model == "typhoon-v1.5x-70b-instruct":
        return "https://api.opentyphoon.ai/v1"
    elif model == "typhoon-v2-70b-instruct":
        return "https://api.opentyphoon.ai/v1"
    elif model == "openai-gpt-3.5-turbo": # ref Docs : https://www.restack.io/p/openai-python-answer-base-url-cat-ai
        return "https://api.opentyphoon.ai/v1"
    else: 
        return "https://api.opentyphoon.ai/v1"

# Session Class
class Session:
    def __init__(self, session_id=None):
        self.session_id = session_id or str(uuid.uuid4())
        self.created_at = datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')
        self.messages = []
        self.uploaded_file = None
        self.file_path = None
        self.last_activity = self.created_at
        
    def to_dict(self):
        return {
            'session_id': self.session_id,
            'created_at': self.created_at,
            'messages': self.messages,
            'file_path': self.file_path,
            'last_activity': self.last_activity
        }
    
    @classmethod
    def from_dict(cls, data):
        session = cls(session_id=data['session_id'])
        session.created_at = data['created_at']
        session.messages = data['messages']
        session.file_path = data['file_path']
        session.last_activity = data.get('last_activity', datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S'))
        return session

    def update_activity(self):
        self.last_activity = datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')

# Session Manager Class
class SessionManager:
    def __init__(self, base_dir=BASE_SESSION_DIR):
        self.base_dir = base_dir
        self.ensure_base_dir()
        
    def ensure_base_dir(self):
        os.makedirs(self.base_dir, exist_ok=True)
        
    def get_session_dir(self, session_id):
        return os.path.join(self.base_dir, session_id)
    
    def create_session(self):
        session = Session()
        session_dir = self.get_session_dir(session.session_id)
        os.makedirs(session_dir, exist_ok=True)
        self.save_session(session)
        return session
    
    def save_session(self, session):
        session_dir = self.get_session_dir(session.session_id)
        os.makedirs(session_dir, exist_ok=True)
        session_file = os.path.join(session_dir, 'session.json')
        with open(session_file, 'w') as f:
            json.dump(session.to_dict(), f)
    
    def load_session(self, session_id):
        session_dir = self.get_session_dir(session_id)
        session_file = os.path.join(session_dir, 'session.json')
        if os.path.exists(session_file):
            with open(session_file, 'r') as f:
                try:
                    data = json.load(f)
                    return Session.from_dict(data)
                except json.JSONDecodeError:
                    return None
        return None
    
    
    def delete_session(self, session_id):
        session_dir = self.get_session_dir(session_id)
        if os.path.exists(session_dir):
            shutil.rmtree(session_dir)
    
    def list_sessions(self):
        if not os.path.exists(self.base_dir):
            return []
        sessions = []
        for session_id in os.listdir(self.base_dir):
            session = self.load_session(session_id)
            if session:
                sessions.append(session)
        return sorted(sessions, key=lambda x: x.last_activity, reverse=True)

# File Management Functions
def save_uploaded_file(uploaded_file, session_id):
    if uploaded_file is not None:
        session_dir = os.path.join(BASE_SESSION_DIR, session_id)
        os.makedirs(session_dir, exist_ok=True)
        
        file_path = os.path.join(session_dir, uploaded_file.name)
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        return file_path
    return None

def delete_session_file(session_id):
    session_dir = os.path.join(BASE_SESSION_DIR, session_id)
    if os.path.exists(session_dir):
        for file in os.listdir(session_dir):
            if file != 'session.json':
                os.remove(os.path.join(session_dir, file))

@st.cache_data
def load_data(file_path, session_id):
    data_handler = DataHandler({})
    file_key = os.path.splitext(os.path.basename(file_path))[0]
    data_handler.dataset_paths[file_key] = file_path
    data_handler.load_data()
    data_handler.preprocess_data()
    return data_handler

# Initialize session state
if 'session_manager' not in st.session_state:
    st.session_state['session_manager'] = SessionManager()
if 'current_session' not in st.session_state:
    st.session_state['current_session'] = None
if 'data_handler' not in st.session_state:
    st.session_state['data_handler'] = DataHandler({})
if 'typhoon_agent' not in st.session_state:
    st.session_state['typhoon_agent'] = None
if 'messages' not in st.session_state:
    st.session_state['messages'] = []
if 'initial_message_sent' not in st.session_state:
    st.session_state['initial_message_sent'] = False 

# Session Management Functions
def start_new_session():
    session = st.session_state['session_manager'].create_session()
    st.session_state['current_session'] = session
    st.success(f"Started new session: {session.session_id[:8]}")
    return session

def switch_session(session_id: str, selected_model: str, temperature: float):
    try:
        # Retrieve the session
        session = st.session_state['session_manager'].load_session(session_id)
        if not session:
            raise ValueError(f"Session with ID {session_id} not found.")

        # Update the current session
        st.session_state['current_session'] = session
        st.session_state['data_handler'] = load_data(session.file_path, session.session_id)
        
        # Initialize TyphoonAgent with the new session details
        dataset_key = os.path.splitext(os.path.basename(session.file_path))[0]
        st.session_state['typhoon_agent'] = TyphoonAgent(
            temperature=temperature,
            base_url=get_model_base_url(selected_model),
            model_name=selected_model,
            dataset_paths={dataset_key: session.file_path},
            dataset_key=dataset_key,
            session_id=session.session_id
        )
        logging.info(f"Switched to session {session_id} with dataset {dataset_key}")

    except Exception as e:
        logging.error(f"Error switching session: {str(e)}")
        logging.error(traceback.format_exc())
        st.error(f"Error switching session: {str(e)}")

def delete_current_session():
    if st.session_state['current_session']:
        session_id = st.session_state['current_session'].session_id
        st.session_state['session_manager'].delete_session(session_id)
        st.session_state['current_session'] = None
        st.session_state['typhoon_agent'] = None
        st.session_state['data_handler'] = DataHandler({})
        st.success(f"Session {session_id[:8]} deleted")

def clear_chat_history():
    if st.session_state['current_session']:
        st.session_state['current_session'].messages = []
        st.session_state['session_manager'].save_session(st.session_state['current_session'])
        if st.session_state['typhoon_agent']:
            st.session_state['typhoon_agent'].clear_memory()
        st.success("Chat history cleared")

# Handle message submission
def handle_submit(user_input):
    if not st.session_state['current_session']:
        st.warning("Please start or select a session first")
        return
        
    if user_input.strip() and st.session_state['typhoon_agent']:
        current_session = st.session_state['current_session']
        current_session.update_activity()
        
        # Add user message
        message = {
            "role": "user",
            "content": user_input,
            "timestamp": datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')
        }
        current_session.messages.append(message)
        st.session_state['messages'].append(message)
        
        try:
            with st.spinner("🤖 Assistant is typing..."):
                # Get response from Typhoon Agent
                response = st.session_state['typhoon_agent'].run(user_input)
                
                # Add assistant message
                message = {
                    "role": "assistant",
                    "content": response.model_dump(),
                    "timestamp": datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')
                }
                current_session.messages.append(message)
                st.session_state['messages'].append(message)
                
                # Save session
                st.session_state['session_manager'].save_session(current_session)
                
        except Exception as e:
            st.error(f"Error: {str(e)}")
        
        # Rerun the script to update the chat interface
        st.rerun()

# Main Application Layout
def main():
    st.title(APP_NAME)
        # LLM log example
        # Console log display
    # Console log display
    with st.expander("Console logs.", expanded=False):
        if st.session_state['current_session']:
            for message in st.session_state['current_session'].messages:
                if message["role"] == "assistant" and "raw_response" in message["content"]:
                    raw_response = message["content"]["raw_response"]
                    if raw_response:
                        formatted_text = re.sub(r"(Thought:|Final Answer:|Action:|Action Input:|Observation:|Action Output:)", r"\n\1", raw_response)
                        formatted_text = re.sub(r"<br>\s*<br>", "<br>", formatted_text)
                        st.markdown(formatted_text, unsafe_allow_html=True)
                    else:
                        st.write("No console logs available.")
        else:
            st.write("📜 All chat logs.")
    
    # Sidebar
    with st.sidebar:
        st.title("⚙️ Settings")
        
        # User info
        st.info(f"🕒 UTC: {datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')}")
        # Display current session
        current_session = st.session_state.get('current_session')
        if current_session:
            st.subheader("Current Session")
            st.info(f"Session ID: {current_session.session_id[:8]}")
        
        # Session management
        st.header("Session Management")
        if st.button("🆕 New Chat Session"):
            start_new_session()

        model_options = {
            "typhoon-v1.5x": "typhoon-v1.5x-70b-instruct",
            "typhoon-v2": "typhoon-v2-70b-instruct",
            "open_ai": "typhoon-v2-70b-instruct"
        }
        selected_model:str = st.selectbox("🦾 Model Settings", list(model_options.values()))
        
        # Temperature slider
        temperature:float = st.select_slider(
            'Set temperature',
            options=[round(i * 0.1, 1) for i in range(0, 11)],
            value=0.3
        )
        
        # Session list
        sessions = st.session_state['session_manager'].list_sessions()
        if sessions:
            st.subheader("Your Sessions")
            for session in sessions:
                with st.container():
                    col1, col2, col3 = st.columns([2, 1, 1])
                    with col1:
                        st.info(f"Session {session.session_id[:8]}\n{session.last_activity}")
                    with col2:
                        if st.button("Switch", key=f"switch_{session.session_id}"):
                            switch_session(session.session_id, selected_model, temperature)
                    with col3:
                        if st.button("Delete", key=f"delete_{session.session_id}"):
                            if st.session_state['current_session'] and session.session_id == st.session_state['current_session'].session_id:
                                delete_current_session()
                            else:
                                st.session_state['session_manager'].delete_session(session.session_id)
        
        # File management for current session
        if st.session_state['current_session']:
            st.header("File Management")
            current_session = st.session_state['current_session']
            
            if not current_session.file_path:
                uploaded_file = st.file_uploader(
                    "📂 Upload Dataset",
                    type=['csv', 'xls', 'xlsx'],
                    key='file_uploader'
                )

                if uploaded_file:
                    file_path = save_uploaded_file(uploaded_file, current_session.session_id)
                    st.session_state['initial_message_sent'] = False
                    if file_path:
                        try:
                            data_handler = load_data(file_path, current_session.session_id)
                            st.session_state['data_handler'] = data_handler
                            current_session.file_path = file_path
                            
                            # Initialize TyphoonAgent
                            dataset_key = os.path.splitext(uploaded_file.name)[0]
                            st.session_state['typhoon_agent'] = TyphoonAgent(
                                temperature=temperature,
                                base_url=get_model_base_url(selected_model),
                                model_name=selected_model,
                                dataset_paths={dataset_key: file_path},
                                dataset_key=dataset_key,
                                session_id=current_session.session_id
                            )
                            st.session_state['session_manager'].save_session(current_session)
                            st.success(f"Successfully loaded {uploaded_file.name}")
                            if not st.session_state['initial_message_sent']:
                                handle_submit(user_input="🔛 start Assistant system.")
                                st.session_state['initial_message_sent'] = True
                        except Exception as e:
                            st.error(f"Error loading file: {str(e)}")
            else:
                st.info(f"Current file: {os.path.basename(current_session.file_path)}")

    # Chat interface
    if st.session_state['current_session']:
        # Display chat messages
        for message in st.session_state['current_session'].messages:
            with st.container():
                if message["role"] == "user":
                    st.markdown(f"""
                    <div class="user-message">
                        <div style="display: flex; justify-content: space-between;">
                            <div>{message["content"]}</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    # Check for sub_response and pandas_agent
                    if "sub_response" in message["content"] and "pandas_agent" in message["content"]["sub_response"]:
                        pandas_response = message["content"]["sub_response"]["pandas_agent"]
                        if pandas_response.get("execution_result"):
                            if "plots" in pandas_response["execution_result"] and pandas_response["execution_result"]["plots"]:
                                for plot in pandas_response["execution_result"]["plots"]:
                                    with st.container():
                                        st.markdown("🤖 Assistant:")
                                        st.image(os.path.join("static", "plots", plot["filename"]), width=800)
                                        with st.expander("Show plot details"):
                                            st.markdown('</div>', unsafe_allow_html=True)
                                            st.code(pandas_response["execution_result"]["output"])    
                            elif "output" in pandas_response["execution_result"]:
                                st.markdown("🤖 Assistant:")
                                st.code(pandas_response["execution_result"]["output"])

                        # Display any additional content (code, plots, etc.)
                        if pandas_response.get("code"):
                            with st.expander("Show Code"):
                                st.code(pandas_response["code"], language="python")

                        if pandas_response.get("explanation"):
                            st.markdown("🤖 Assistant:")
                            explan = pandas_response["explanation"].get("explanation", "")
                            st.write(translate_func(target_lang='th', text=explan))
                    else:
                        st.markdown(f"""
                        <div class="assistant-messagel">
                            <div style="display: flex; justify-content: space-between;">
                                <div>🤖 Assistant:{message["content"].get("response", "")}</div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

        # Chat input
        user_input = st.chat_input(
            key='user_input',
            placeholder="Type your message and press Enter"
        )
        if user_input:
            handle_submit(user_input)
    else:
        st.info("Please start a new chat session or select an existing one.")

if __name__ == "__main__":
    main()
