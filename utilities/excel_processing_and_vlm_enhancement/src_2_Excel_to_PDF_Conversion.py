# Databricks notebook source
# MAGIC %md
# MAGIC # Excel to PDF Conversion (Stage 2)
# MAGIC
# MAGIC Second stage of Excel processing pipeline:
# MAGIC - Reads metadata table from Stage 1
# MAGIC - Applies filters for file selection
# MAGIC - Converts Excel files to PDF using LibreOffice
# MAGIC - Updates metadata table with conversion results
# MAGIC
# MAGIC **Prerequisites**: Run notebook 1 first
# MAGIC **Output**: PDF files and updated metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Environment Setup

# COMMAND ----------

# MAGIC %sh
# MAGIC apt-get update > /dev/null 2>&1
# MAGIC DEBIAN_FRONTEND=noninteractive apt-get install -y libreoffice > /dev/null 2>&1
# MAGIC echo "LibreOffice installed successfully"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parameter Configuration

# COMMAND ----------

# Create widgets for job parameters
dbutils.widgets.text("catalog", "main", "1. Catalog Name")
dbutils.widgets.text("schema", "default", "2. Schema Name")
dbutils.widgets.text("metadata_table", "excel_metadata", "3. Metadata Table Name")
dbutils.widgets.text("pdf_output_volume", "excel_pdfs", "4. PDF Output Volume Name")
dbutils.widgets.dropdown("filter_type", "all", ["all", "has_chart", "has_image", "has_visual_element", "custom"], "5. Filter Type")
dbutils.widgets.text("custom_filter", "", "6. Custom SQL Filter (optional)")
dbutils.widgets.dropdown("conversion_mode", "pending_only", ["pending_only", "all", "failed_only"], "7. Conversion Mode")

# Get parameter values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
metadata_table = dbutils.widgets.get("metadata_table")
pdf_output_volume = dbutils.widgets.get("pdf_output_volume")
filter_type = dbutils.widgets.get("filter_type")
custom_filter = dbutils.widgets.get("custom_filter")
conversion_mode = dbutils.widgets.get("conversion_mode")

# Construct paths
metadata_table_name = f"{catalog}.{schema}.{metadata_table}"
pdf_output_path = f"/Volumes/{catalog}/{schema}/{pdf_output_volume}"

# Print configuration
print("=" * 80)
print("PDF CONVERSION CONFIGURATION")
print("=" * 80)
print(f"Metadata Table:         {metadata_table_name}")
print(f"PDF Output Path:        {pdf_output_path}")
print(f"Filter Type:            {filter_type}")
print(f"Conversion Mode:        {conversion_mode}")
if custom_filter:
    print(f"Custom Filter:          {custom_filter}")
print("=" * 80)

# COMMAND ----------

import json
json.dumps(dbutils.widgets.getAll())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Import Libraries

# COMMAND ----------

import os
import json
import tempfile
import shutil
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Optional

from pyspark.sql.functions import col, lit, current_timestamp, when
from pyspark.sql.types import StringType, LongType, TimestampType

print("All libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. PDF Conversion Function

# COMMAND ----------

def convert_excel_to_pdf(
    excel_path: str,
    pdf_output_dir: str
) -> Tuple[str, bool, Optional[str]]:
    """
    Convert Excel to PDF using LibreOffice headless mode.
    
    Args:
        excel_path: Path to Excel file
        pdf_output_dir: PDF output directory
    Returns:
        Tuple of (pdf_path, success, error_message)
    """
    temp_dir = None
    
    try:
        # Create temporary directory for conversion
        temp_dir = tempfile.mkdtemp()
        
        # Copy Excel file from volume to temp location using dbutils
        excel_filename = os.path.basename(excel_path)
        temp_excel = os.path.join(temp_dir, excel_filename)
        
        # Use dbutils to copy from volume to local temp
        dbutils.fs.cp(excel_path, f"file:{temp_excel}")
        
        # Verify Excel file was copied
        if not os.path.exists(temp_excel):
            shutil.rmtree(temp_dir, ignore_errors=True)
            return (None, False, "Failed to copy Excel file to temp location")
        
        # Construct LibreOffice command (convert in temp directory)
        cmd = [
            "soffice",
            "--headless",
            "--convert-to", "pdf",
            "--outdir", temp_dir,
            temp_excel
        ]
        
        # Execute conversion
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=300  # 5 minute timeout
        )
        
        ## Determine temp PDF path
        excel_stem = Path(excel_filename).stem
        temp_pdf = os.path.join(temp_dir, f"{excel_stem}.pdf")
        
        # Verify PDF was created
        if not os.path.exists(temp_pdf):
            shutil.rmtree(temp_dir, ignore_errors=True)
            return (None, False, "PDF file not created by LibreOffice")
        
        # Ensure output directory exists
        os.makedirs(pdf_output_dir, exist_ok=True)
        
        # Copy PDF from temp to volume using dbutils
        final_pdf_path = os.path.join(pdf_output_dir, f"{excel_stem}.pdf")
        dbutils.fs.cp(f"file:{temp_pdf}", final_pdf_path, recurse=True)
        
        # Cleanup temp directory
        shutil.rmtree(temp_dir, ignore_errors=True)
        
        return (final_pdf_path, True, None)
            
    except subprocess.TimeoutExpired:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
        return (None, False, "Conversion timeout (5 minutes)")
    except subprocess.CalledProcessError as e:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
        return (None, False, f"LibreOffice error: {e.stderr}")
    except Exception as e:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
        return (None, False, str(e))


def get_pdf_file_info(pdf_path: str) -> Dict:
    """
    Get PDF file size information.
    
    Args:
        pdf_path: Path to PDF file
    Returns:
        Dictionary with file metadata
    """
    try:
        if pdf_path and os.path.exists(pdf_path):
            pdf_stat = os.stat(pdf_path)
            return {
                'pdf_file_size_bytes': pdf_stat.st_size,
                'pdf_file_size_mb': round(pdf_stat.st_size / (1024 * 1024), 2)
            }
        else:
            return {
                'pdf_file_size_bytes': None,
                'pdf_file_size_mb': None
            }
    except Exception:
        return {
            'pdf_file_size_bytes': None,
            'pdf_file_size_mb': None
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Query Metadata Table with Filters

# COMMAND ----------

# Build WHERE clause based on filter type
where_conditions = []

# Add conversion mode filter
if conversion_mode == "pending_only":
    where_conditions.append("pdf_conversion_status = 'pending'")
elif conversion_mode == "failed_only":
    where_conditions.append("pdf_conversion_status = 'failed'")
elif conversion_mode == "all":
    where_conditions.append("preparation_status = 'success'")

# Add visual element filters
if filter_type == "has_chart":
    where_conditions.append("has_chart = true")
elif filter_type == "has_image":
    where_conditions.append("has_image = true")
elif filter_type == "has_visual_element":
    where_conditions.append("has_visual_element = true")
elif filter_type == "custom" and custom_filter:
    where_conditions.append(f"({custom_filter})")

# Combine conditions
where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

print(f"Query filter: {where_clause}")
print()

# Query bronze table for prepared Excel files
query = f"""
SELECT 
    b.document_id,
    regexp_replace(b.path, '^dbfs:', '') as source_file_path,
    b.file_name as source_file_name,
    m.pdf_ready_excel_path,
    COALESCE(m.has_chart, false) as has_chart,
    COALESCE(m.has_image, false) as has_image,
    COALESCE(m.has_visual_element, false) as has_visual_element,
    COALESCE(m.total_charts, 0) as total_charts,
    COALESCE(m.total_images, 0) as total_images,
    COALESCE(m.total_tables, 0) as total_tables,
    m.preparation_status,
    m.pdf_conversion_status
FROM {catalog}.{schema}.bronze_documents b
INNER JOIN {metadata_table_name} m ON regexp_replace(b.path, '^dbfs:', '') = m.source_file_path
WHERE b.file_type = 'spreadsheet'
    AND b.processing_status = 'prepared'
    AND m.pdf_ready_excel_path IS NOT NULL
    AND {where_clause}
"""

print("Querying bronze layer for prepared Excel files...")
files_to_convert_df = spark.sql(query)
files_to_convert = files_to_convert_df.collect()

print(f"Found {len(files_to_convert)} file(s) to convert")
print()

if len(files_to_convert) == 0:
    print("No files match the filter criteria")
    dbutils.notebook.exit(json.dumps({
        "status": "completed",
        "total_files": 0,
        "message": "No files to convert"
    }))

# Display files to be converted
print("Files to convert:")
display(files_to_convert_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Batch PDF Conversion

# COMMAND ----------

# Create output directory
os.makedirs(pdf_output_path, exist_ok=True)
print("PDF output directory created/verified")
print()

print("=" * 80)
print("STARTING PDF CONVERSION")
print("=" * 80)

# Process each file
conversion_results = []

for idx, row in enumerate(files_to_convert, 1):
    source_file_name = row['source_file_name']
    pdf_ready_path = row['pdf_ready_excel_path']
    
    print(f"\n[{idx}/{len(files_to_convert)}] Converting: {source_file_name}")
    print("-" * 80)
    
    start_time = datetime.now()
    
    result = {
        'source_file_path': row['source_file_path'],
        'source_file_name': source_file_name,
        'pdf_file_path': None,
        'pdf_file_size_bytes': None,
        'pdf_file_size_mb': None,
        'conversion_status': 'success',
        'conversion_error': None,
        'conversion_duration': None
    }
    
    try:
        if not pdf_ready_path:
            result['conversion_status'] = 'failed'
            result['conversion_error'] = "No PDF-ready Excel file available"
            print(f"Status: ✗ FAILED - No PDF-ready Excel file")
        else:
            # Convert to PDF
            pdf_result = convert_excel_to_pdf(pdf_ready_path, pdf_output_path)
            
            if pdf_result[1]:  # Success
                result['pdf_file_path'] = pdf_result[0]
                
                # Get PDF file info
                pdf_info = get_pdf_file_info(result['pdf_file_path'])
                result['pdf_file_size_bytes'] = pdf_info['pdf_file_size_bytes']
                result['pdf_file_size_mb'] = pdf_info['pdf_file_size_mb']
                
                print(f"Status: ✓ SUCCESS")
                if result['pdf_file_size_mb']:
                    print(f"PDF Size: {result['pdf_file_size_mb']} MB")
            else:
                result['conversion_status'] = 'failed'
                result['conversion_error'] = pdf_result[2]
                print(f"Status: ✗ FAILED")
                print(f"Error: {pdf_result[2]}")
    
    except Exception as e:
        result['conversion_status'] = 'failed'
        result['conversion_error'] = str(e)
        print(f"Status: ✗ FAILED")
        print(f"Error: {str(e)}")
    
    # Calculate duration
    end_time = datetime.now()
    result['conversion_duration'] = (end_time - start_time).total_seconds()
    print(f"Duration: {result['conversion_duration']:.2f}s")
    
    conversion_results.append(result)

print()
print("=" * 80)
print("PDF CONVERSION COMPLETED")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Update Metadata Table

# COMMAND ----------

print("\nUpdating metadata table...")

# Create update DataFrame
from pyspark.sql import Row

update_rows = []
for result in conversion_results:
    row = Row(
        source_file_path=result['source_file_path'],
        pdf_file_path=result['pdf_file_path'],
        pdf_file_size_bytes=result['pdf_file_size_bytes'],
        pdf_file_size_mb=str(result['pdf_file_size_mb']) if result['pdf_file_size_mb'] is not None else None,
        pdf_conversion_status=result['conversion_status'],
        pdf_conversion_error=result['conversion_error'],
        pdf_conversion_timestamp=datetime.now()
    )
    update_rows.append(row)

from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType; schema_data = StructType([StructField('source_file_path', StringType(), True), StructField('pdf_file_path', StringType(), True), StructField('pdf_file_size_bytes', LongType(), True), StructField('pdf_file_size_mb', StringType(), True), StructField('pdf_conversion_status', StringType(), True), StructField('pdf_conversion_error', StringType(), True), StructField('pdf_conversion_timestamp', TimestampType(), True)]); update_df = spark.createDataFrame(update_rows, schema=schema_data)

# Read current metadata table
metadata_df = spark.table(metadata_table_name)

# Merge updates into metadata table
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, metadata_table_name)

delta_table.alias("target").merge(
    update_df.alias("updates"),
    "target.source_file_path = updates.source_file_path"
).whenMatchedUpdate(set={
    "pdf_file_path": col("updates.pdf_file_path"),
    "pdf_file_size_bytes": col("updates.pdf_file_size_bytes"),
    "pdf_file_size_mb": col("updates.pdf_file_size_mb"),
    "pdf_conversion_status": col("updates.pdf_conversion_status"),
    "pdf_conversion_timestamp": col("updates.pdf_conversion_timestamp")
}).execute()

print("Metadata table updated successfully")

# Update bronze_documents table with conversion status
print("\nUpdating bronze layer with conversion status...")
try:
    # Create mapping of source_file_path to document_id from original query
    path_to_doc_id = {row.source_file_path: row.document_id for row in files_to_convert}
    
    for result in conversion_results:
        file_path = result['source_file_path']
        if file_path in path_to_doc_id:
            document_id = path_to_doc_id[file_path]
            pdf_path = result.get('pdf_file_path', '')
            status = 'converted' if result['conversion_status'] == 'success' else 'conversion_failed'
            
            # Update with final PDF path and conversion status
            query = f"""
                UPDATE {catalog}.{schema}.bronze_documents
                SET processing_status = '{status}',
                    converted_pdf_path = '{pdf_path}',
                    conversion_timestamp = current_timestamp()
                WHERE document_id = '{document_id}'
            """

            spark.sql(query)
    print("Bronze layer updated successfully")
except Exception as e:
    print(f"WARNING: Failed to update bronze layer: {str(e)}")
    print("Metadata table was updated successfully, but bronze status update failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Conversion Summary

# COMMAND ----------

print("\n" + "=" * 80)
print("CONVERSION SUMMARY")
print("=" * 80)

# Calculate statistics
total_conversions = len(conversion_results)
successful_conversions = sum(1 for r in conversion_results if r['conversion_status'] == 'success')
failed_conversions = sum(1 for r in conversion_results if r['conversion_status'] == 'failed')
total_pdf_size_mb = sum(r['pdf_file_size_mb'] or 0 for r in conversion_results)
total_duration = sum(r['conversion_duration'] or 0 for r in conversion_results)

print(f"\nTotal Files Converted:        {total_conversions}")
print(f"Successful Conversions:       {successful_conversions}")
print(f"Failed Conversions:           {failed_conversions}")
print()
print(f"Total PDF Size:               {total_pdf_size_mb:.2f} MB")
print(f"Total Conversion Time:        {total_duration:.2f} seconds")
if total_conversions > 0:
    print(f"Average Time per File:        {total_duration/total_conversions:.2f} seconds")
print()
print(f"Output Locations:")
print(f"  PDF Files:                  {pdf_output_path}")
print(f"  Updated Metadata Table:     {metadata_table_name}")
print()

# Show failed conversions if any
if failed_conversions > 0:
    print("\nFailed Conversions:")
    print("-" * 80)
    for result in conversion_results:
        if result['conversion_status'] == 'failed':
            print(f"  • {result['source_file_name']}")
            print(f"    - {result['conversion_error']}")

print("=" * 80)

# COMMAND ----------


# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Job Completion

# COMMAND ----------

# Create job summary for return value
job_summary = {
    "status": "completed",
    "total_files": total_conversions,
    "successful": successful_conversions,
    "failed": failed_conversions,
    "total_pdf_size_mb": round(total_pdf_size_mb, 2),
    "pdf_output_path": pdf_output_path,
    "metadata_table": metadata_table_name,
    "conversion_time_seconds": round(total_duration, 2)
}

print("\n" + "=" * 80)
print("JOB COMPLETED SUCCESSFULLY")
print("=" * 80)
print()
print(f"✅ {successful_conversions} PDF(s) generated")
print(f"📁 Location: {pdf_output_path}")
print(f"📊 Metadata updated in: {metadata_table_name}")
print("=" * 80)

# Return job summary as notebook output
dbutils.notebook.exit(json.dumps(job_summary))
