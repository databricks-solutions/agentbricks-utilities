# AI Parse Document Pipeline

This pipeline processes documents (PDFs and images) using Databricks AI functions with structured streaming for scalable, incremental document processing.

## What It Does

The pipeline has three stages:

1. **Document Parsing** (`01_parse_documents.py`): Uses `ai_parse_document()` to extract raw content from PDFs and images, handling text, tables, and images with structured streaming. Outputs to `parsed_documents_raw`.

2. **Text Extraction** (`02_extract_text.py`): Extracts clean text from the parsed documents, handling both v1.0 and v2.0 formats. Reads from `parsed_documents_raw` and outputs to `parsed_documents_text`.

3. **Structured Extraction** (`03_extract_structured_data.py`): Uses `ai_query()` to extract structured JSON data from parsed text, including entities, dates, amounts, and summaries. Reads from `parsed_documents_text` and outputs to `parsed_documents_structured`.

## Prerequisites

- Databricks Runtime 13.3+ with Unity Catalog
- Access to Databricks AI functions (`ai_parse_document`, `ai_query`)
- Source volume containing documents
- Destination volumes for parsed output and checkpoints

## Quick Start

### 1. Parse Documents

Run `01_parse_documents.py` with these parameters:

- `catalog`: Unity Catalog name (default: `main`)
- `schema`: Schema name (default: `default`)
- `source_volume_path`: Path to documents (e.g., `/Volumes/main/default/source_docs`)
- `output_volume_path`: Path for extracted images (e.g., `/Volumes/main/default/parsed_images`)
- `checkpoint_location`: Checkpoint path (e.g., `/Volumes/main/default/_checkpoints/parse_documents`)
- `table_name`: Output table name (default: `parsed_documents_raw`)
- `numFilesPerTrigger`: Files to process per batch (default: `10`)

### 2. Extract Text

Run `02_extract_text.py` with these parameters:

- `catalog`: Unity Catalog name (default: `main`)
- `schema`: Schema name (default: `default`)
- `source_table_name`: Table from step 1 (default: `parsed_documents_raw`)
- `checkpoint_location`: Checkpoint path (e.g., `/Volumes/main/default/_checkpoints/extract_text`)
- `table_name`: Output table name (default: `parsed_documents_text`)
- `numFilesPerTrigger`: Files to process per batch (default: `10`)

### 3. Extract Structured Data

Run `03_extract_structured_data.py` with these parameters:

- `catalog`: Unity Catalog name (default: `main`)
- `schema`: Schema name (default: `default`)
- `source_table_name`: Table from step 2 (default: `parsed_documents_text`)
- `checkpoint_location`: Checkpoint path (e.g., `/Volumes/main/default/_checkpoints/extract_structured`)
- `table_name`: Output table name (default: `parsed_documents_structured`)
- `numFilesPerTrigger`: Files to process per batch (default: `10`)

## Configuration

All notebooks use `availableNow=True` trigger mode for batch-like processing. Adjust `numFilesPerTrigger` to control processing rate and resource usage.

## Output

- **Parsed documents** (`parsed_documents_raw`): Table with `path`, `parsed` (variant type), and `parsed_at` timestamp
- **Extracted text** (`parsed_documents_text`): Table with `path`, `text`, `error_status`, and `parsed_at` timestamp
- **Structured data** (`parsed_documents_structured`): Table with `path`, `extracted_json`, `parsed_at`, and `extraction_timestamp`

