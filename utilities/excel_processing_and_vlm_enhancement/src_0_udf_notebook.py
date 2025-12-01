# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog UDFs for Document Processing
# MAGIC
# MAGIC Creates UDF for VLM processing:
# MAGIC - `crop_and_encode_image_impl` - Crops and encodes images for VLM
# MAGIC
# MAGIC **Note**: Excel-to-PDF conversion is handled by src_1 and src_2 notebooks

# COMMAND ----------

# MAGIC %pip install pillow --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "dbxmetagen")
dbutils.widgets.text("schema", "default")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## UDF: Crop and Encode Image
# MAGIC
# MAGIC Crops image from binary content to bounding box and returns binary JPEG.
# MAGIC Serializable UDF that works on Spark workers.

# COMMAND ----------

# Use IDENTIFIER to make the function creation dynamic based on parameters
spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.crop_and_encode_image_impl(
    x1 INTEGER, y1 INTEGER, x2 INTEGER, y2 INTEGER, content BINARY
)
RETURNS BINARY
LANGUAGE PYTHON
ENVIRONMENT (
  dependencies = '["pillow"]',
  environment_version = 'None'
)
AS $$
import base64
from PIL import Image
import io
img = Image.open(io.BytesIO(content))
img.load()
cropped = img.crop((x1, y1, x2, y2))
buffer = io.BytesIO()
cropped.save(buffer, format="JPEG")
return buffer.getvalue()
$$
""")

print(f"✓ Registered {catalog}.{schema}.crop_and_encode_image_impl")

# COMMAND ----------

print(f"\n{'='*60}")
print(f"UDF Registration Complete")
print(f"{'='*60}")
print(f"Catalog:  {catalog}")
print(f"Schema:   {schema}")
print(f"Function: crop_and_encode_image_impl")
print(f"{'='*60}")
print(f"\nNext Steps:")
print(f"1. Run src_1_Excel_Preparation_and_Detection.py (if processing Excel)")
print(f"2. Run src_2_Excel_to_PDF_Conversion.py (if processing Excel)")
print(f"3. Run src_Main Pipeline.sql")
print(f"{'='*60}")