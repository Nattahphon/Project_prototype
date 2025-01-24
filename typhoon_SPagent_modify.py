# -----------------------------------------------------------------------
# this is main
import locale
try:
    locale.setlocale(locale.LC_ALL, 'th_TH.UTF-8')
except locale.Error:
    locale.setlocale(locale.LC_ALL, '')  # ใช้ locale เริ่มต้นของระบบ
import logging
import re
import sys
from typing import Optional
sys.stdout.reconfigure(encoding='utf-8')
import contextlib
import io
import json
from dotenv import load_dotenv
import os
from langchain.agents import AgentExecutor, create_react_agent
from langchain.memory import ConversationBufferMemory
from langchain_openai import ChatOpenAI
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
from tabulate import tabulate
from Z_pandas_tst import PandasAgent
from H_datahandle_app import DataHandler
from langchain_core.tools import Tool
# from H_summary_app import SummaryAgent
from pydantic import BaseModel, Field
from datetime import datetime
import pytz
from langchain_core.prompts import PromptTemplate
import seaborn as sns
from langchain_core.output_parsers import JsonOutputParser
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

# Add these constants at the top of your file, after the imports
STATIC_DIR = "static"
PLOT_DIR = os.path.join(STATIC_DIR, "plots")
PLOT_URL_PREFIX = "/static/plots"  # This will be used for frontend access

class PlotInfo(BaseModel):
    """Model for plot file information"""
    filename: str
    path: str
    created_at: str

class ExecutionResult(BaseModel):
    """Model for code execution results"""
    output: Optional[str] = None
    error: Optional[str] = None
    plots: List[PlotInfo] = []

class SubResponseContent(BaseModel):
    """Model for tool subresponse content"""
    code: Optional[str] = None
    execution_result: Optional[ExecutionResult] = None
    explanation: Optional[Dict[str, Any]] = None
    type: str = "tool_response"
    response: Optional[str] = None

class MetaData(BaseModel):
    """Model for response metadata"""
    timestamp: str
    user: str
    model: str
    temperature: float
    tools_used: List[str]
    dataset_key: str
    status: str = "success"

class TyphoonResponse(BaseModel):
    """Main response model"""
    query: str
    response: str
    raw_response: Optional[str] = None  # Add this line
    sub_response: Dict[str, SubResponseContent]
    plot_data: Dict[str, List[PlotInfo]]
    metadata: MetaData
    error: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class PlotResponse(BaseModel):
    query: str = Field(description="Description of what is user query")
    response: str = Field(description="Description of output from TyphoonAgent")
    sub_response: dict = Field(description="Response from tool that TyphoonAgent uses")

    def dict(self, *args, **kwargs):
        """Override dict method to customize the output format"""
        return {
            "query": self.query,
            "response": self.response,
            "sub_response": self.sub_response
        }

class TyphoonAgent:
    def __init__(self, temperature: float, base_url: str, model_name: str, dataset_paths: dict, dataset_key: str, session_id: str):
        self.temperature = temperature
        self.base_url = base_url
        self.model = model_name
        self.dataset_key = dataset_key
        self.api_key = os.getenv("TYPHOON_API_KEY")
        self.api_sub_key = os.getenv("PLOT_API_KEY")
        self.session_id = session_id
        self.llm = self.initialize_llm()
        self.llms = self.initialize_sub_llm()
        self.memory = self.initialize_memory()
        self.pandas_agent = PandasAgent(temperature, base_url, model_name, dataset_paths, session_id)
        # self.summary_agent = SummaryAgent(temperature, base_url, model_name)
        self.tools = self.initialize_tools()
        self.agent = self.create_agent()
        self.agent_executor = self.create_agent_executor()
        self.output_parser = JsonOutputParser()

    def initialize_llm(self) -> ChatOpenAI:
        """Initialize the language model."""
        return ChatOpenAI(
            base_url=self.base_url,
            model=self.model,
            api_key=self.api_key,
            temperature=self.temperature,
            top_p = 0.95
        )
    

    def initialize_sub_llm(self) -> ChatOpenAI:
        """Initialize the language model."""
        return ChatOpenAI(
            base_url=self.base_url,
            model=self.model,
            api_key=self.api_sub_key,
            temperature=self.temperature,
        )

    def initialize_memory(self):
        """Set up memory for the conversation."""
        return ConversationBufferMemory(memory_key="chat_history", return_messages=True)
    
    def clear_memory(self):
        """Clear the conversation memory."""
        print('Memory cleared !!!')
        return self.memory.clear()

    def initialize_tools(self):
        """Initialize the tools used by the TyphoonAgent."""
        pandas_tool = Tool(
            name="pandas_agent",
            func=self.query_dataframe,
            description=(
                "REQUIRED for ANY data analysis, plotting, or DataFrame operations. "
                "Do not provide direct code - instead, use this tool for all data tasks. "
                "Input: Describe what analysis or plot you want to create. "
                "Output: Will provide code and explanation."
            ),
        )
        return [pandas_tool]
    
    def summary_answer(self, user_input: str) -> None:
        return self.summary_agent.summarize(user_input)
    

    def query_dataframe(self, user_input: str) -> dict:
        """Delegate the user query to the PandasAgent for processing."""
        try:
            result = self.pandas_agent.run_and_return_code(user_input, self.dataset_key)
            if 'code' in result:
                # Replace plt.show() with savefig in the code
                result['code'] = result['code'].replace('plt.show()', '')
                
                # If it's a plotting operation, modify the code to use a single figure
                if 'plt.figure' in result['code'] and '.plot(' in result['code']:
                    # Remove separate plt.figure() call if it exists
                    result['code'] = result['code'].replace('plt.figure(figsize=(10, 6))\n', '')
                    # Add figsize to the plot() call
                    result['code'] = result['code'].replace('.plot(', '.plot(figsize=(10, 6), ')
                    
            return result
        except Exception as e:
            return {
                "error": str(e),
                "query": user_input,
                "explanation": "Error occurred while processing the query"
            }


    def create_agent(self):
        """Create a React agent with dataset-aware prompting and structured response requirements."""
        react_prompt = PromptTemplate.from_template("""
        Assistant is a large language model modified by a DS student.

        Assistant is designed to assist with a wide range of tasks, from answering simple questions to providing in-depth explanations and discussions on diverse topics. It can generate human-like text based on the input it receives, enabling natural-sounding conversations and providing coherent and relevant responses.

        Assistant is constantly learning and improving. It processes and understands large amounts of text to provide accurate and informative responses. Additionally, Assistant can generate its own text based on the input it receives, allowing it to engage in discussions and provide explanations and descriptions on various topics.

        Overall, Assistant is a powerful tool that can help with numerous tasks and provide valuable insights and information. Whether you need help with a specific question or just want to have a conversation about a particular topic, Assistant is here to assist.

        TOOLS:
        ------

        Assistant has access to the following tools:

        {tools}

        To use a tool, please use the following format:
        Thought: Do I need to use a tool? Yes
        Action: the action to take, should be one of [{tool_names}]
        Action Input: the input to the action
        Observation: the result of the action

        When you have a response to say to the Human, or if you do not need to use a tool, you MUST use the format:
        Thought: Do I need to use a tool? No
        Final Answer: [your response here]
        Begin!

        Previous conversation history:
        {chat_history}

        New input: {input}
        {agent_scratchpad}
        """)

        # Get the current dataset information
        if self.dataset_key not in self.pandas_agent.handler._data:
            raise ValueError(f"Dataset '{self.dataset_key}' not found.")

        df = self.pandas_agent.handler.get_data(self.dataset_key)
        
        custom_prefix = f"""You are a Data Analysis Supervisor specializing in DataFrame operations.
        CURRENT DATASET: {self.dataset_key}
        AVAILABLE COLUMNS: {', '.join(df.columns)}

        IMPORTANT RULES:
        1. If user query is related to interact with df, dataframe you have to interact with CURRENT DATASET name: {self.dataset_key}
        2. NEVER provide code directly in your response
        3. ALWAYS use pandas_agent for ANY data analysis, plotting, or DataFrame operations
        4. Your main response should be brief and reference the tool outputs
        5. Work with the DataFrame that has these columns: {', '.join(df.columns)}
        
        Remember: ALL code must come from tools, never in direct response."""

        custom_suffix = f"""
        Important now you have CURRENT DATASET: {self.dataset_key} from pandas_agent
        RESPONSE STRUCTURE REQUIREMENTS:
        1. Main Response Format:
        - Keep responses concise and clear
        - Reference the specific tool outputs
        - Maintain exact query consistency
        - Use proper JSON structure
        - Consider available columns: {', '.join(df.columns)}

        2. Tool Usage Guidelines:
        - Pass the exact original query to tools
        - Do not modify or rephrase user queries
        - Use pandas_agent for ALL data operations
        - Use summary_agent for result explanations

        3. Error Handling:
        - Report any issues in processing
        - Maintain consistent error response format
        - Include explanation of errors
        - Check column availability before operations

        4. Dataset Context:
        - Working with dataset: {self.dataset_key}
        - Available columns: {', '.join(df.columns)}
        - Ensure operations use existing columns
        - Validate data types before operations

        5. Additional Guidelines:
        - Never expose internal implementation details
        - Keep responses focused on data analysis
        - Ensure all code comes from tools
        - Maintain professional tone
        - Consider column data types for operations
        """
        
        react_prompt = react_prompt.partial(
            system_message=custom_prefix + custom_suffix
        )
        
        return create_react_agent(llm=self.llm, tools=self.tools, prompt=react_prompt)

    def create_agent_executor(self):
        """Create the agent executor to handle queries."""
        return AgentExecutor(
            agent=self.agent,
            tools=self.tools,
            memory=self.memory,
            verbose=True,
            max_iterations=int(os.getenv("MAX_ITERATIONS", 3)),
            handle_parsing_errors=True,
            return_intermediate_steps=True
        )


    def _prepare_plot_code(self, code: str) -> str:
        """Prepare the code for execution by removing plt.show() and ensuring proper plot saving."""
        # Remove plt.show() if it exists
        code = code.replace('plt.show()', '')
        return code
    

    def execute_code(self, code: str) -> ExecutionResult:
        """Safely execute Python code and capture plots."""
        # Clear any existing plots
        plt.close('all')
        
        # Prepare the plot directory
        plot_dir = "static/plots"
        os.makedirs(plot_dir, exist_ok=True)
        current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Prepare the code by removing plt.show()
        code = self._prepare_plot_code(code)
        
        context = {
            "pd": pd, 
            "np": np, 
            "sns": sns, 
            "plt": plt, 
            "tabulate": tabulate, 
            "df": self.pandas_agent.handler.get_data(self.dataset_key)
        }
        
        output = io.StringIO()
        plot_files = []
        
        with contextlib.redirect_stdout(output):
            try:
                # Execute the modified code
                exec(code, context)
                
                # Save any generated plots
                if plt.get_fignums():
                    # Only get the last figure if multiple exist
                    fig = plt.gcf()  # Get current figure
                    filename = f"plot_{current_time}.png"
                    filepath = os.path.join(plot_dir, filename)
                    
                    # Save the plot with high quality settings
                    fig.savefig(filepath, 
                            bbox_inches='tight',
                            dpi=300,
                            format='png')
                    
                    # Create plot info
                    plot_files.append(PlotInfo(
                        filename=filename,
                        path=f"/static/plots/{filename}",
                        created_at=datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')
                    ))
                    plt.close(fig)  # Close the figure to free memory
                
                return ExecutionResult(
                    output=output.getvalue(),
                    plots=plot_files
                )
                
            except Exception as e:
                # Clean up any open figures in case of error
                plt.close('all')
                return ExecutionResult(
                    error=str(e),
                    plots=[]
                )

    
    def get_explanation(self, output) -> dict:
        """Get explanation for the output using Typhoon LLM."""
        
        prompt = PromptTemplate(
            template="""
        Your task is to analyze the provided output and deliver a detailed explanation to the user. The explanation should:
        - Be clear, concise, and accurate.
        - Provide context and cover all relevant aspects of the analysis.
        - Highlight key insights or takeaways effectively.
        - Include examples or implications where applicable to improve understanding.
        - Be tailored to the user's needs, ensuring the explanation is actionable and easy to follow.

        Return the explanation as a JSON object with the key 'explanation'.

        {format_instructions}
        {output}
        """,
            input_variables=["output"],
            partial_variables={"format_instructions": self.output_parser.get_format_instructions()},
        )
        try:
            chain = prompt | self.llms | self.output_parser
            response = chain.invoke({"output": output,})
            explanation = response
        except Exception as e:
            explanation = {"error": f"Error getting explanation: {e}", "raw_output": output}
        
        # Ensure the explanation is valid JSON
        if isinstance(explanation, str):
            try:
                json_explanation = json.loads(explanation)
            except json.JSONDecodeError:
                json_explanation = {"error": "Invalid JSON output", "raw_output": explanation}
        elif isinstance(explanation, dict):
            json_explanation = explanation
        else:
            json_explanation = {"error": "Unexpected output type", "raw_output": explanation}
        
        return json_explanation

    # def decode_unicode(self, data):
    #     """Decode Unicode escape sequences in a Python dictionary."""
    #     if isinstance(data, str):
    #         return data
    #     elif isinstance(data, dict):
    #         return {k: self.decode_unicode(v) for k, v in data.items()}
    #     elif isinstance(data, list):
    #         return [self.decode_unicode(v) for v in data]
    #     else:
    #         return data
        
    def _process_tool_output(self, output) -> Dict[str, Any]:
        """Process tool output into a standardized format."""
        if isinstance(output, dict):
            return output
        elif isinstance(output, str):
            # Try to parse JSON if it looks like JSON
            if output.strip().startswith('{'):
                try:
                    return json.loads(output)
                except json.JSONDecodeError:
                    return {"response": output, "type": "text_response"}
            else:
                return {"response": output, "type": "text_response"}
        else:
            return {"response": str(output), "type": "converted_response"}


    def run(self, user_input: str) -> TyphoonResponse:
        """Run the agent and return structured response."""
        try:
            df = self.pandas_agent.handler.get_data(self.dataset_key)
            input_query = f"""
            User Query: {user_input}

            Please analyze the query carefully using critical thinking. 
            If the query relates to data analysis, consider the following:
            - Current dataset : {self.dataset_key}
            - Available columns: {', '.join(df.columns)}

            Provide an accurate and insightful response to address the user's query.
            """

            # raw_response = self.agent_executor.invoke({"input": input_query})
                    # Get the raw response from the agent
            verbose_output = io.StringIO()
            with contextlib.redirect_stdout(verbose_output):
                raw_response = self.agent_executor.invoke({"input": user_input}, verbose=True) #, callbacks=[callback]
            
            main_response = raw_response.get('output', '')
            intermediate_steps = raw_response.get('intermediate_steps', [])
            sub_response = {}
            plot_data = {"plots": []}
            verbose_content = verbose_output.getvalue()
            verbose_output.close()

            # Regular Expression
            pattern = r"Thought:.*?Finished chain\..*"

            # ค้นหาข้อความ
            match = re.search(pattern, verbose_content, re.DOTALL)
            if match:
                extracted_text = match.group(0)
                extracted_text = extracted_text.rsplit("Finished chain.", 1)[0].strip()
                extracted_text = re.sub(r"\[\d+[;]?\d*m|> ?", "", extracted_text)
                #print(extracted_text)
            else:
                extracted_text = verbose_content
                #print("ไม่พบข้อความที่ตรงกัน")
            for step in intermediate_steps:
                if len(step) >= 2:
                    tool_name = step[0].tool
                    tool_output = self._process_tool_output(step[1])
                    
                    if tool_name == "pandas_agent" and "code" in tool_output:
                        execution_result = self.execute_code(tool_output["code"])
                        explanation = self.get_explanation(
                            execution_result.output if execution_result.output 
                            else execution_result.error
                        )
                        
                        sub_response[tool_name] = SubResponseContent(
                            code=tool_output["code"],
                            execution_result=execution_result,
                            explanation=explanation
                        )
                        
                        if execution_result.plots:
                            plot_data["plots"].extend(execution_result.plots)
                    else:
                        sub_response[tool_name] = SubResponseContent(
                            type="tool_response",
                            response=str(tool_output)
                        )

            metadata = MetaData(
                timestamp=datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S'),
                user=os.getenv("USER", "Nattahphon"),
                model=self.model,
                temperature=self.temperature,
                tools_used=list(sub_response.keys()),
                dataset_key=self.dataset_key
            )

            return TyphoonResponse(
                query=user_input,
                response=main_response,
                raw_response=extracted_text,
                sub_response=sub_response,
                plot_data=plot_data,
                metadata=metadata
            )

        except Exception as e:
            error_metadata = MetaData(
                timestamp=datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S'),
                user=os.getenv("USER", "Nattahphon"),
                model=self.model,
                temperature=self.temperature,
                tools_used=[],
                dataset_key=self.dataset_key,
                status="error"
            )
            
            return TyphoonResponse(
                query=user_input,
                response="Error occurred during processing",
                sub_response={},
                plot_data={"plots": []},
                metadata=error_metadata,
                error=str(e)
            )


if __name__ == '__main__':
    load_dotenv()
    sys.stdin.reconfigure(encoding='utf-8')
    sys.stdout.reconfigure(encoding='utf-8')
    
    # Make sure static directory exists
    os.makedirs(STATIC_DIR, exist_ok=True)
    os.makedirs(PLOT_DIR, exist_ok=True)
    
    file_paths = {
        "Financials": "./temp_uploads/Financials.csv",
    }

    typhoon_agent = TyphoonAgent(
        temperature=0.1,
        base_url="https://api.opentyphoon.ai/v1",
        model_name="typhoon-v2-70b-instruct",
        dataset_paths=file_paths,
        dataset_key="Financials"
    )

    handler = DataHandler(dataset_paths=file_paths)
    handler.load_data()
    handler.preprocess_data()

    while True:
        user_input = input("Enter your query: ")
        if user_input.lower() == "stop agent":
            typhoon_agent.clear_memory()
            print("Exiting Typhoon Agent...")
            break
            
        try:
            result = typhoon_agent.run(user_input)

            # Display full results
            print("\n=== Full Results ===")
            result_dict = result.model_dump()
            print(json.dumps(result_dict, indent=2, ensure_ascii=False))
            
            # Display specific sections
            print("\n=== Query Results ===")
            print(f"Query: {result.query}")
            print(f"Response: {result.response}")
            print(f"Verbose: {result.raw_response}")
            
            if "pandas_agent" in result.sub_response:
                pandas_response = result.sub_response["pandas_agent"]
                
                print("\n=== Code ===")
                if pandas_response.code:
                    print(pandas_response.code)
                
                if pandas_response.execution_result:
                    print("\n=== Execution Output ===")
                    if pandas_response.execution_result.output:
                        print(pandas_response.execution_result.output)
                    if pandas_response.execution_result.error:
                        print(f"Error: {pandas_response.execution_result.error}")
                    
                    # Display plot information
                    if pandas_response.execution_result.plots:
                        print("\n=== Generated Plots ===")
                        for plot in pandas_response.execution_result.plots:
                            print(f"Plot filename: {plot.filename}")
                            print(f"Plot URL path: {plot.path}")
                            print(f"Created at: {plot.created_at}")
                            print("---")
                
                if pandas_response.explanation:
                    print("\n=== Explanation ===")
                    print(pandas_response.explanation.get('explanation', ''))
            
            print("\n=== Metadata ===")
            print(json.dumps(result.metadata.model_dump(), indent=2, ensure_ascii=False))
            
        except Exception as e:
            print("\n=== Error ===")
            print(f"Error processing result: {str(e)}")
            if hasattr(result, 'model_dump'):
                print(json.dumps(result.model_dump(), indent=2, ensure_ascii=False))