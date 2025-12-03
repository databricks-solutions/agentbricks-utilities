# Databricks notebook source
# MAGIC %md
# MAGIC # Document RAG Agent with Mosaic AI
# MAGIC
# MAGIC This notebook creates a RAG agent using Databricks Mosaic AI Agent Framework.
# MAGIC It processes documents from the gold layer and enables natural language querying.
# MAGIC
# MAGIC **Prerequisites**: Run stages 0-3 to populate gold_final_documents table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Required Packages

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow langchain langgraph==0.3.4 databricks-langchain pydantic databricks-agents unitycatalog-langchain[databricks]
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Create widgets
dbutils.widgets.text("catalog", "guanyu_chen", "Catalog")
dbutils.widgets.text("schema", "kie", "Schema")
dbutils.widgets.text("vector_search_endpoint", "endpoint-poc", "Vector Search Endpoint")

# Get parameters
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
vector_search_endpoint_name = dbutils.widgets.get("vector_search_endpoint")

# Set up paths
gold_table_name = f"{catalog}.{schema}.gold_final_documents"
chunks_table_name = f"{catalog}.{schema}.document_chunks"
vector_index_name = f"{catalog}.{schema}.document_chunks_vs_index"
agent_model_name = f"{catalog}.{schema}.document_rag_agent"

print(f"Configuration:")
print(f"  Gold table: {gold_table_name}")
print(f"  Chunks table: {chunks_table_name}")
print(f"  Vector index: {vector_index_name}")
print(f"  Agent model: {agent_model_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Parse Documents and Create Chunks

# COMMAND ----------

from pyspark.sql import functions as F

# Read and parse documents using VARIANT : operator
gold_df = spark.table(gold_table_name)

# Extract text from VARIANT column similar to the example
docs_df = gold_df.select(
    F.regexp_replace("path", "^dbfs:", "").alias("doc_uri"),
    F.expr("""
        concat_ws(
            '\n\n',
            transform(
                try_cast(vlm_enhanced_parsed_data:document:elements AS ARRAY<VARIANT>),
                e -> try_cast(e:content AS STRING)
            )
        )
    """).alias("content")
).filter(F.length("content") > 50)

print(f"✓ Parsed {docs_df.count()} documents from {gold_df.count()} total documents")
display(docs_df.limit(3))

# COMMAND ----------

# Chunk the documents
from langchain.text_splitter import RecursiveCharacterTextSplitter
from pyspark.sql.types import ArrayType, StringType

def chunk_text(text):
    """Chunk text into smaller pieces."""
    if not text or len(text.strip()) == 0:
        return []
    
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=800,
        chunk_overlap=100,
        separators=["\n\n", "\n", ". ", " ", ""]
    )
    return splitter.split_text(text)

chunk_udf = F.udf(chunk_text, ArrayType(StringType()))

# Create chunks with doc_uri and unique chunk_id
chunks_df = docs_df.select(
    "doc_uri",
    F.explode(chunk_udf("content")).alias("content")
).withColumn(
    "chunk_id",
    F.expr("uuid()")
).filter(F.length("content") > 50)

# Save to table with Change Data Feed enabled (required for Vector Search)
chunks_df.write \
    .mode("overwrite") \
    .option("delta.enableChangeDataFeed", "true") \
    .saveAsTable(chunks_table_name)

# Enable Change Data Feed on the table (required for Delta Sync Index)
spark.sql(f"ALTER TABLE {chunks_table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

print(f"✓ Created {chunks_df.count()} chunks from {docs_df.count()} documents")
print(f"✓ Change Data Feed enabled on {chunks_table_name}")
display(spark.table(chunks_table_name).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Vector Search Index

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

# Initialize Vector Search client
vsc = VectorSearchClient(disable_notice=True)

# Create or verify endpoint
print("Checking vector search endpoint...")
try:
    endpoint = vsc.get_endpoint(vector_search_endpoint_name)
    print(f"✓ Endpoint '{vector_search_endpoint_name}' exists")
    print(f"  Status: {endpoint.get('endpoint_status', {}).get('state', 'unknown')}")
except Exception as e:
    print(f"Endpoint '{vector_search_endpoint_name}' not found. Creating...")
    try:
        vsc.create_endpoint(
            name=vector_search_endpoint_name,
            endpoint_type="STANDARD"
        )
        print(f"✓ Created endpoint: {vector_search_endpoint_name}")
        
        # Wait for endpoint to be online
        print("  Waiting for endpoint to be ready...")
        import time
        for i in range(60):  # Wait up to 10 minutes
            try:
                endpoint = vsc.get_endpoint(vector_search_endpoint_name)
                state = endpoint.get('endpoint_status', {}).get('state', 'UNKNOWN')
                if state == 'ONLINE':
                    print(f"  ✓ Endpoint is ONLINE")
                    break
                print(f"    [{i*10}s] State: {state}")
            except:
                pass
            time.sleep(10)
    except Exception as create_error:
        print(f"❌ Error creating endpoint: {create_error}")
        print("Please check:")
        print("  1. You have permissions to create vector search endpoints")
        print("  2. Your workspace has Vector Search enabled")
        print("  3. The endpoint name is valid")
        raise

# COMMAND ----------

# Create or get existing index
print(f"\nSetting up vector search index: {vector_index_name}")
print(f"  Source table: {chunks_table_name}")
print(f"  Embedding model: databricks-bge-large-en")

try:
    # Check if index exists
    try:
        index = vsc.get_index(index_name=vector_index_name)
        print(f"✓ Using existing index: {vector_index_name}")
    except:
        # Create new Delta Sync Index
        print(f"  Creating new index...")
        index = vsc.create_delta_sync_index(
            endpoint_name=vector_search_endpoint_name,
            source_table_name=chunks_table_name,
            index_name=vector_index_name,
            pipeline_type="TRIGGERED",
            primary_key="chunk_id",
            embedding_source_column="content",
            embedding_model_endpoint_name="databricks-bge-large-en"
        )
        print(f"✓ Vector search index created successfully!")
    
    print(f"  Index: {vector_index_name}")
    
except Exception as e:
    print(f"❌ Error with index: {e}")
    print("\nTroubleshooting checklist:")
    print(f"  1. ✓ Endpoint exists: {vector_search_endpoint_name}")
    print(f"  2. ? Table has Change Data Feed enabled: {chunks_table_name}")
    print("     Run: SHOW TBLPROPERTIES " + chunks_table_name)
    print("  3. ? You have CREATE TABLE privileges on the schema")
    print("  4. ? Embedding endpoint 'databricks-bge-large-en' is available")
    raise

# COMMAND ----------

# Trigger initial sync (required for TRIGGERED pipeline type)
print("\nTriggering initial index sync...")
try:
    index.sync()
    print("✓ Sync triggered")
except Exception as e:
    print(f"Note: {e}")

# COMMAND ----------

# Wait for index to be ready
import time

print("\nWaiting for index to be ready...")
max_wait = 600  # 10 minutes
start_time = time.time()

while (time.time() - start_time) < max_wait:
    try:
        idx = vsc.get_index(index_name=vector_index_name)
        status = idx.describe()
        
        # Check if index is ready
        index_status = status.get('status', {})
        if index_status.get('ready', False):
            print("✓ Index is ready!")
            print(f"  Indexed rows: {status.get('num_indexed_rows', 'N/A')}")
            break
        
        # Show progress
        state = index_status.get('detailed_state', 'UNKNOWN')
        message = index_status.get('message', '')
        elapsed = int(time.time() - start_time)
        print(f"  [{elapsed}s] State: {state}")
        if message:
            print(f"         {message}")
            
    except Exception as e:
        print(f"  Checking status... ({int(time.time() - start_time)}s)")
    
    time.sleep(10)

# Final status check
try:
    final_status = vsc.get_index(vector_index_name).describe()
    print(f"\nFinal index status:")
    print(f"  Ready: {final_status.get('status', {}).get('ready', False)}")
    print(f"  Indexed rows: {final_status.get('num_indexed_rows', 0)}")
except:
    print("Could not retrieve final status")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Build Agent

# COMMAND ----------

import mlflow
from databricks import agents

# Set MLflow registry to Unity Catalog
mlflow.set_registry_uri("databricks-uc")

# Enable autologging
mlflow.langchain.autolog()

print("✓ MLflow configured")

# COMMAND ----------

# Build agent using Databricks Vector Search retriever
from databricks_langchain import ChatDatabricks
from langchain_community.vectorstores import DatabricksVectorSearch
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser

# Initialize LLM
llm = ChatDatabricks(endpoint="databricks-llama-4-maverick")

# Get the index object (reuse vsc from earlier)
vs_index = vsc.get_index(
    endpoint_name=vector_search_endpoint_name,
    index_name=vector_index_name
)

# Create vector search retriever
vectorstore = DatabricksVectorSearch(
    index=vs_index,
    text_column="content",
    columns=["chunk_id", "doc_uri", "content"]
)
retriever = vectorstore.as_retriever(search_kwargs={"k": 5})

print("✓ Retriever configured")

# COMMAND ----------

# Define RAG prompt
rag_prompt = ChatPromptTemplate.from_messages([
    ("system", """You are a helpful AI assistant that answers questions about documents.
    
Use the provided context to answer the user's question accurately and concisely.

Guidelines:
- Answer based ONLY on the provided context
- Cite the source documents in your answer
- If the context doesn't contain the answer, say "I don't have enough information to answer this question"
- Be clear and concise

Context: {context}"""),
    ("user", "{question}")
])

# Create RAG chain
def format_docs(docs):
    """Format retrieved documents for context."""
    formatted = []
    for i, doc in enumerate(docs, 1):
        doc_uri = doc.metadata.get('doc_uri', 'Unknown')
        content = doc.page_content
        formatted.append(f"[Document {i}: {doc_uri}]\n{content}\n")
    return "\n".join(formatted)

rag_chain = (
    {
        "context": retriever | format_docs,
        "question": RunnablePassthrough()
    }
    | rag_prompt
    | llm
    | StrOutputParser()
)

print("✓ RAG chain created")

# COMMAND ----------

# Test the RAG chain
test_question = "What is Agile at Baguide?"
print(f"Testing RAG chain with: {test_question}")
print("=" * 80)

response = rag_chain.invoke(test_question)
print("\nResponse:")
print(response)
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Log and Register Agent

# COMMAND ----------

# Define loader function for model serving
def load_retriever(persist_dir=None):
    """
    Creates and returns the vector search retriever.
    This function will be called when the model is loaded for serving.
    """
    from databricks.vector_search.client import VectorSearchClient
    from langchain_community.vectorstores import DatabricksVectorSearch
    from databricks_langchain import ChatDatabricks
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_core.runnables import RunnablePassthrough
    from langchain_core.output_parsers import StrOutputParser
    
    # Configuration
    endpoint_name = vector_search_endpoint_name
    index_name = vector_index_name
    
    # Initialize Vector Search client
    vsc = VectorSearchClient(disable_notice=True)
    
    # Get the index
    vs_index = vsc.get_index(
        endpoint_name=endpoint_name,
        index_name=index_name
    )
    
    # Create vectorstore and retriever
    vectorstore = DatabricksVectorSearch(
        index=vs_index,
        text_column="content",
        columns=["chunk_id", "doc_uri", "content"]
    )
    retriever = vectorstore.as_retriever(search_kwargs={"k": 5})
    
    # Initialize LLM
    llm = ChatDatabricks(endpoint="databricks-llama-4-maverick")
    
    # Create prompt
    rag_prompt = ChatPromptTemplate.from_messages([
        ("system", """You are a helpful AI assistant that answers questions about documents.
        
Use the provided context to answer the user's question accurately and concisely.

Guidelines:
- Answer based ONLY on the provided context
- Cite the source documents in your answer
- If the context doesn't contain the answer, say "I don't have enough information to answer this question"
- Be clear and concise

Context: {context}"""),
        ("user", "{question}")
    ])
    
    # Format docs function
    def format_docs(docs):
        formatted = []
        for i, doc in enumerate(docs, 1):
            doc_uri = doc.metadata.get('doc_uri', 'Unknown')
            content = doc.page_content
            formatted.append(f"[Document {i}: {doc_uri}]\n{content}\n")
        return "\n".join(formatted)
    
    # Create and return the chain
    chain = (
        {
            "context": retriever | format_docs,
            "question": RunnablePassthrough()
        }
        | rag_prompt
        | llm
        | StrOutputParser()
    )
    
    return chain

print("✓ Loader function defined")

# COMMAND ----------

# Log the RAG chain as an agent
with mlflow.start_run(run_name="document_rag_agent") as run:
    # Log the chain with loader_fn
    logged_chain_info = mlflow.langchain.log_model(
        lc_model=rag_chain,
        artifact_path="chain",
        input_example="What information is in the documents?",
        loader_fn=load_retriever,  # This recreates the chain when model is loaded
        pip_requirements=[
            "mlflow",
            "langchain",
            "databricks-vectorsearch", 
            "databricks-langchain",
            "pydantic"
        ]
    )
    run_id = run.info.run_id

print(f"✓ RAG chain logged to MLflow with loader function")
print(f"  Run ID: {run_id}")
print(f"  Model URI: {logged_chain_info.model_uri}")

# COMMAND ----------

# Register to Unity Catalog
registered_model = mlflow.register_model(
    model_uri=logged_chain_info.model_uri,
    name=agent_model_name
)

print(f"✓ Model registered to Unity Catalog")
print(f"  Model: {agent_model_name}")
print(f"  Version: {registered_model.version}")
# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Evaluate Agent with MLflow GenAI

# COMMAND ----------

# Import evaluation tools
import pandas as pd
from mlflow.genai.evaluation import evaluate


# Create evaluation dataset with questions
eval_data = pd.DataFrame({
   "inputs": [
       {"prompt": "What types of documents are in the system?"},
       {"prompt": "Are there any charts or visualizations?"},
       {"prompt": "What information is contained in tables?"},
       {"prompt": "Summarize the main topics"},
       {"prompt": "What are the key findings?"}
   ]
})


print(f"✓ Created evaluation dataset with {len(eval_data)} examples")
display(eval_data)

# COMMAND ----------

# Define model wrapper function for evaluation
def generate_response(prompt):
   """Wrapper function to call the RAG chain for evaluation"""
   try:
       response = rag_chain.invoke(prompt)
       return response
   except Exception as e:
       return f"Error: {str(e)}"


print("✓ Model wrapper function defined")

# COMMAND ----------

from mlflow.genai.scorers import Safety, RelevanceToQuery


print("Running agent evaluation with MLflow GenAI...")


# Define scorers to evaluate
scorers = [
   Safety(),  # Built-in safety/toxicity detection
   RelevanceToQuery(),  # Answer relevance to the question
]


with mlflow.start_run(run_name="agent_evaluation") as run:
   # Evaluate the agent with scorers
   eval_results = mlflow.genai.evaluate(
       data=eval_data,  # Your evaluation dataset
       predict_fn=generate_response,  # Your RAG chain function
       scorers=scorers,  # Use scorers parameter, not metrics
   )
  
   print("\n✓ Evaluation complete!")
   print(f"  Run ID: {run.info.run_id}")
  
   # Display aggregate metrics
   print("\n" + "="*80)
   print("EVALUATION METRICS")
   print("="*80)
  
   if eval_results.metrics:
       for metric_name, metric_value in eval_results.metrics.items():
           if isinstance(metric_value, (int, float)):
               print(f"  {metric_name}: {metric_value:.3f}")
           else:
               print(f"  {metric_name}: {metric_value}")
  
   print("="*80)


# COMMAND ----------

# View detailed evaluation results
print("\nViewing detailed evaluation results...")

# Display the evaluation results table
if hasattr(eval_results, 'tables') and 'eval_results_table' in eval_results.tables:
    results_table = eval_results.tables['eval_results_table']
    print(f"\n✓ Evaluated {len(results_table)} examples")
    display(results_table)
else:
    print("Results available in MLflow UI")

# COMMAND ----------

# Instructions for viewing and next steps
print("\n" + "="*80)
print("VIEW RESULTS & NEXT STEPS")
print("="*80)
print("\n1. MLflow UI:")
print(f"   - Go to MLflow Experiments in Databricks")
print(f"   - Find run ID: {run.info.run_id}")
print(f"   - Check the 'Evaluation' tab for detailed results")
print("\n2. Deploy to Review App:")
print("   - Use agents.deploy() to deploy the agent")
print("   - Access Review App for interactive testing")
print("\n3. Iterate:")
print("   - Review low-scoring examples")
print("   - Improve prompts or retrieval")
print("   - Re-run evaluation to compare")
print("="*80)
