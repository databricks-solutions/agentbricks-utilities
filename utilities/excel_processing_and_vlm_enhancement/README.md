# Unstructured Document Processing & RAG Pipeline

A complete document processing pipeline on Databricks that converts Excel files to PDF, processes all other supported file types, extracts content with AI, and enables semantic search with VLM and construct a RAG agent.

## 📋 Overview

This pipeline processes documents through multiple stages:
1. **Excel Preparation** - Detects visual elements and prepares Excel files
2. **PDF Conversion** - Converts Excel to PDF using LibreOffice
3. **AI Document Parsing** - Extracts text, tables, and figures with VLM enhancement
4. **RAG Vector Search** - Creates searchable index and Q&A agent

## 🚀 Quick Start

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Cluster with ML runtime (14.3 LTS ML or higher)
- Permissions to create tables, volumes, and vector search endpoints

### Step 1: Setup Unity Catalog

Run the SQL commands to create required volumes and enable Change Data Feed:

```sql
-- Create catalog and schema (if needed)
CREATE CATALOG IF NOT EXISTS your_catalog;
CREATE SCHEMA IF NOT EXISTS your_catalog.your_schema;

-- Create source volume for documents
CREATE VOLUME IF NOT EXISTS your_catalog.your_schema.source_files;

-- Upload your documents to the volume
```

### Step 2: Run the Pipeline

Execute notebooks in order:

#### Option A: SQL Pipeline (Stages 0-3)

Run `src/excel2pdf/src_Main Pipeline.sql` to:
- Create bronze layer (ingest all documents)
- Run notebooks to process Excel files (if any)
- Process PDF files
- Parse documents with AI
- Generate VLM-enhanced descriptions
- Create gold_final_documents table

**Configure these parameters in the SQL notebook:**
```sql
DECLARE catalog STRING DEFAULT 'your_catalog';
DECLARE schema STRING DEFAULT 'your_schema';
DECLARE source_volume STRING DEFAULT 'source_files';
```

#### Option B: Individual Python Notebooks (Excel only)

If you have Excel files, run these in order:

1. **`src_1_Excel_Preparation_and_Detection.py`**
   - Detects charts, images, tables in Excel files
   - Creates PDF-ready versions
   - Outputs: excel_metadata table

2. **`src_2_Excel_to_PDF_Conversion.py`**
   - Converts Excel to PDF using LibreOffice
   - Updates metadata table
   - Outputs: PDF files in volume

#### Stage 4: RAG Vector Search (Optional)

Run `src/excel2pdf/src_4_RAG_Vector_Search.py` to:
- Parse documents and extract text
- Create semantic chunks
- Generate embeddings (BGE model)
- Build vector search index
- Deploy RAG agent
- Run quality evaluation

**Configure these widgets:**
- Catalog: `your_catalog`
- Schema: `your_schema`
- Vector Search Endpoint: `your_endpoint_name` (auto-created if missing)

## 🔧 Configuration

### For SQL Pipeline

Edit variables at the top of `src_Main Pipeline.sql`:

```sql
DECLARE catalog STRING DEFAULT 'your_catalog';
DECLARE schema STRING DEFAULT 'your_schema';
DECLARE source_volume STRING DEFAULT 'source_files';
DECLARE image_volume_path STRING DEFAULT '/Volumes/your_catalog/your_schema/source_files';
```

### For RAG Pipeline

Set widgets in `src_4_RAG_Vector_Search.py`:

| Parameter | Description | Example |
|-----------|-------------|---------|
| catalog | Unity Catalog name | `main` |
| schema | Schema name | `default` |
| vector_search_endpoint | Endpoint name (auto-created) | `my_endpoint` |

## 📊 Output Tables

After running the pipeline, you'll have:

| Table | Description |
|-------|-------------|
| `bronze_documents` | Raw document ingestion |
| `silver_parsed_documents` | AI-parsed documents |
| `gold_final_documents` | VLM-enhanced final documents |
| `document_chunks` | Text chunks for search |
| `gold_document_embeddings` | Vector embeddings |


## 📝 Notes

- **Excel Processing**: Only needed if you have Excel files
- **SQL Pipeline**: Handles all document types (PDF, DOCX, PPTX, Excel)
- **RAG Stage**: Optional, adds semantic search capabilities
- **Evaluation**: Runs automatically with quality metrics

## 🔗 Key Features

✅ **Automatic Resource Creation** - Creates endpoints and tables as needed
✅ **Change Data Feed** - Enabled automatically for vector search
✅ **VLM Enhancement** - AI-generated descriptions for figures
✅ **Quality Evaluation** - Built-in safety and relevance scoring

## 📚 Additional Resources

- [Databricks Vector Search Docs](https://docs.databricks.com/en/generative-ai/vector-search.html)
- [Databricks Agent Framework](https://docs.databricks.com/en/generative-ai/agent-framework/index.html)
- [MLflow GenAI Evaluation](https://docs.databricks.com/en/mlflow/genai-evaluate.html)

## 💡 Tips

- **Start small**: Test with 5-10 documents first
- **Check logs**: Review cell outputs for errors
- **Monitor costs**: Vector search and LLM calls have associated costs
- **Iterate**: Use evaluation results to improve your agent

---

**Questions?** Check the Databricks documentation or review the notebook comments for detailed explanations.
