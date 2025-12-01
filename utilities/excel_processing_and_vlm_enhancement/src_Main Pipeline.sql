-- Databricks SQL Pipeline for Document Processing (Medallion Architecture)
-- 
-- Prerequisites:
-- 1. Run udf_notebook.py to register UDFs
-- 2. Run this pipeline Step 0 to create bronze layer
-- 3. Run src_1 and src_2 notebooks for Excel preprocessing (updates bronze)
-- 4. Run remaining pipeline steps

-- Declare parameters
DECLARE catalog STRING DEFAULT 'guanyu_chen';
DECLARE schema STRING DEFAULT 'kie';
DECLARE source_volume STRING DEFAULT 'source_files';
DECLARE converted_pdf_folder STRING DEFAULT 'excel_pdf';
DECLARE image_volume_path STRING DEFAULT '/Volumes/guanyu_chen/kie/source_files';
DECLARE excel_metadata_table STRING DEFAULT 'excel_metadata';

-- Step 0: Bronze Layer - Load all raw files including Excel
CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.bronze_documents') AS
SELECT 
    uuid() as document_id,
    path,
    content,
    regexp_extract(path, '[^/]+$', 0) as file_name,
    lower(regexp_extract(path, '\\.([^.]+)$', 1)) as file_extension,
    CASE 
        WHEN lower(regexp_extract(path, '\\.([^.]+)$', 1)) IN ('pdf') THEN 'application/pdf'
        WHEN lower(regexp_extract(path, '\\.([^.]+)$', 1)) IN ('pptx', 'ppt') THEN 'presentation'
        WHEN lower(regexp_extract(path, '\\.([^.]+)$', 1)) IN ('docx', 'doc') THEN 'document'
        WHEN lower(regexp_extract(path, '\\.([^.]+)$', 1)) IN ('jpg', 'jpeg', 'png') THEN 'image'
        WHEN lower(regexp_extract(path, '\\.([^.]+)$', 1)) IN ('xlsx', 'xlsm', 'xls') THEN 'spreadsheet'
        ELSE 'unknown'
    END as file_type,
    length(content) as file_size_bytes,
    round(length(content) / 1024 / 1024, 2) as file_size_mb,
    current_timestamp() as ingestion_timestamp,
    current_date() as ingestion_date,
    'ingested' as processing_status,
    CAST(NULL AS STRING) as converted_pdf_path,
    CAST(NULL AS TIMESTAMP) as conversion_timestamp
FROM read_files(
    CONCAT('/Volumes/', :catalog, '/', :schema, '/', :source_volume, '/*.*'),
    format => 'binaryFile'
)
WHERE LOWER(path) RLIKE '\\.(pdf|jpe?g|png|docx?|pptx?|xlsx?|xlsm)$';

-- Step 1: Silver Layer - Parse documents from bronze
-- For Excel: uses converted PDFs from src_1/src_2 processing
-- For others: uses original content from bronze
CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.silver_parsed_documents') AS
WITH converted_pdfs AS (
    -- Read all converted Excel PDFs from volume
    SELECT 
        path,
        content
    FROM read_files(
        CONCAT('/Volumes/', :catalog, '/', :schema, '/', :source_volume, '/', :converted_pdf_folder, '/*'),
        format => 'binaryFile'
    )
    WHERE LOWER(path) RLIKE '\\.pdf$'
),
files_to_parse AS (
    SELECT 
        b.document_id,
        b.path as original_path,
        b.file_type,
        b.file_name,
        b.file_size_mb,
        b.ingestion_timestamp,
        -- For converted Excel, use PDF content from converted_pdfs
        -- For others, use original content from bronze
        CASE 
            WHEN b.file_type = 'spreadsheet' AND b.converted_pdf_path IS NOT NULL
            THEN c.content
            ELSE b.content
        END as content_to_parse,
        CASE 
            WHEN b.file_type = 'spreadsheet' THEN 'excel_converted'
            ELSE 'direct'
        END as source_type
    FROM IDENTIFIER(:catalog || '.' || :schema || '.bronze_documents') b
    LEFT JOIN converted_pdfs c ON regexp_replace(c.path, '^dbfs:', '') = b.converted_pdf_path
    WHERE b.file_type != 'spreadsheet'  -- Non-Excel files
       OR (b.file_type = 'spreadsheet' AND b.processing_status = 'converted')  -- Converted Excel
)
SELECT 
    document_id,
    original_path as path,
    source_type,
    ai_parse_document(
        content_to_parse,
        map('version', '2.0',
            'imageOutputPath', 
            :image_volume_path,
            'descriptionElementTypes', '*'
        )
    ) as parsed_data
FROM files_to_parse;

-- Step 1b: Ingest JPEG content from parsed images
CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.jpeg_content') AS
SELECT * FROM read_files(:image_volume_path, format => 'binaryFile');

-- Step 2: Create comprehensive metadata table
CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.document_processing_metadata') AS
WITH parsed_info AS (
    SELECT 
        regexp_replace(path, '^dbfs:', '') as file_path,
        source_type,
        regexp_extract(path, '[^/]+$', 0) as file_name,
        regexp_extract(path, '\\.([^.]+)$', 1) as file_type,
        length(CAST(parsed_data AS STRING)) as file_size_bytes,
        current_timestamp() as processing_timestamp,
        CASE 
            WHEN parsed_data IS NOT NULL AND parsed_data:error IS NULL THEN 'success'
            ELSE 'failed'
        END as ai_parse_status,
        CAST(parsed_data:error AS STRING) as ai_parse_error
    FROM IDENTIFIER(:catalog || '.' || :schema || '.silver_parsed_documents')
),
excel_info AS (
    SELECT 
        CAST(pdf_file_path AS STRING) as file_path,
        has_visual_element,
        CAST(preparation_status AS STRING) as preparation_status,
        CAST(pdf_conversion_status AS STRING) as pdf_conversion_status,
        CAST(source_file_path AS STRING) as original_excel_path
    FROM IDENTIFIER(:catalog || '.' || :schema || '.' || :excel_metadata_table)
    WHERE pdf_file_path IS NOT NULL
)
SELECT 
    p.file_path,
    p.file_name,
    p.file_type,
    p.file_size_bytes,
    p.source_type,
    p.processing_timestamp,
    p.ai_parse_status,
    p.ai_parse_error,
    e.has_visual_element,
    e.preparation_status,
    e.pdf_conversion_status,
    e.original_excel_path,
    CAST(NULL AS INT) as vlm_figures_count,
    CAST(NULL AS INT) as vlm_processed_count,
    CAST(NULL AS STRING) as ai_query_status
FROM parsed_info p
LEFT JOIN excel_info e ON p.file_path = e.file_path;

-- Step 3: VLM Enhancement Creation (Consolidated Steps 3-8)
-- Combines figure extraction, filtering, cropping, and VLM description in one atomic operation
CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.vlm_enhancements') AS
WITH parsed_figures AS (
    -- Extract and explode figure elements from parsed documents
    SELECT 
        path,
        parsed_data,
        posexplode(
            CAST(parsed_data:document:elements AS ARRAY<VARIANT>)
        ) as (element_pos, element)
    FROM IDENTIFIER(:catalog || '.' || :schema || '.silver_parsed_documents')
),
vlm_inputs AS (
    -- Filter for figures only and extract bounding box coordinates
    SELECT 
        path,
        element_pos,
        element:id as element_id,
        element:bbox[0]:page_id as page_id,
        element:bbox[0]:coord[0] as x1,
        element:bbox[0]:coord[1] as y1,
        element:bbox[0]:coord[2] as x2,
        element:bbox[0]:coord[3] as y2,
        element:type as element_type,
        parsed_data
    FROM parsed_figures
    WHERE CAST(element:type AS STRING) = 'figure'
        AND element:bbox IS NOT NULL
),
pages_exploded AS (
    -- Explode pages array to get image URIs
    SELECT 
        path,
        page:id as page_id,
        page:image_uri as image_uri
    FROM IDENTIFIER(:catalog || '.' || :schema || '.silver_parsed_documents') 
    LATERAL VIEW explode(CAST(parsed_data:document:pages AS ARRAY<VARIANT>)) page_table AS page
),
vlm_with_paths AS (
    -- Join figures with pages and JPEG content to get image data
    SELECT 
        v.path,
        CAST(v.element_id AS INTEGER) as element_id,
        CAST(v.page_id AS INTEGER) as page_id,
        CAST(v.x1 AS INTEGER) as x1,
        CAST(v.y1 AS INTEGER) as y1,
        CAST(v.x2 AS INTEGER) as x2,
        CAST(v.y2 AS INTEGER) as y2,
        CAST(p.image_uri AS STRING) as image_path,
        j.content
    FROM vlm_inputs v
    INNER JOIN pages_exploded p 
        ON v.path = p.path 
        AND CAST(v.page_id AS INT) = CAST(p.page_id AS INT)
    INNER JOIN IDENTIFIER(:catalog || '.' || :schema || '.jpeg_content') j 
        ON regexp_replace(CAST(j.path AS STRING), '^dbfs:', '') = CAST(p.image_uri AS STRING)
    WHERE p.image_uri IS NOT NULL
),
vlm_cropped AS (
    -- Crop images and encode to base64
    SELECT 
        path,
        element_id,
        page_id,
        image_path,
        IDENTIFIER(:catalog || '.' || :schema || '.crop_and_encode_image_impl')(
            CAST(x1 AS INT),
            CAST(y1 AS INT),
            CAST(x2 AS INT),
            CAST(y2 AS INT),
            content
        ) as cropped_base64
    FROM vlm_with_paths
)
-- Final select: Get VLM descriptions using AI_QUERY
SELECT 
    path,
    element_id,
    page_id,
    image_path,
    cropped_base64,
    AI_QUERY(
        'databricks-llama-4-maverick',
        'Analyze this chart/image and provide a concise summary in 2-3 sentences. Focus on: 1) What type of visualization it is (chart, diagram, image, etc.), 2) Key data insights, trends, or patterns shown, 3) Main takeaway or conclusion. Avoid describing visual styling details like colors or formatting unless directly relevant to understanding the data.',
        files => cropped_base64
    ) as vlm_description
FROM vlm_cropped
WHERE cropped_base64 IS NOT NULL
    AND cropped_base64 != '';

-- Step 4: Update metadata table with VLM tracking information
MERGE INTO IDENTIFIER(:catalog || '.' || :schema || '.document_processing_metadata') AS target
USING (
    SELECT 
        regexp_replace(path, '^dbfs:', '') as file_path,
        COUNT(DISTINCT element_id) as vlm_figures_count,
        SUM(CASE WHEN vlm_description IS NOT NULL THEN 1 ELSE 0 END) as vlm_processed_count,
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN vlm_description IS NOT NULL THEN 1 ELSE 0 END) THEN 'success'
            WHEN SUM(CASE WHEN vlm_description IS NOT NULL THEN 1 ELSE 0 END) > 0 THEN 'partial'
            ELSE 'failed'
        END as ai_query_status
    FROM IDENTIFIER(:catalog || '.' || :schema || '.vlm_enhancements')
    GROUP BY path
) AS source
ON target.file_path = source.file_path
WHEN MATCHED THEN UPDATE SET
    target.vlm_figures_count = source.vlm_figures_count,
    target.vlm_processed_count = source.vlm_processed_count,
    target.ai_query_status = source.ai_query_status;

-- Step 5: Gold Layer - Final output with VLM enhancements
-- Creates table with original, vlm_enhanced_figures array, and vlm_enhanced_parsed_data
CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.gold_final_documents') AS
WITH vlm_map AS (
    -- Create mapping of element_id to vlm_description for each document
    SELECT 
        path,
        MAP_FROM_ENTRIES(
            COLLECT_LIST(
                STRUCT(CAST(element_id AS STRING), CAST(vlm_description AS STRING))
            )
        ) as vlm_desc_map
    FROM IDENTIFIER(:catalog || '.' || :schema || '.vlm_enhancements')
    WHERE vlm_description IS NOT NULL
    GROUP BY path
),
vlm_figures_agg AS (
    -- Aggregate VLM enhanced figures array
    SELECT 
        path,
        COLLECT_LIST(
            STRUCT(
                element_id as element_id,
                page_id as page_id,
                image_path as image_path,
                vlm_description as enhanced_description
            )
        ) as vlm_enhanced_figures
    FROM IDENTIFIER(:catalog || '.' || :schema || '.vlm_enhancements')
    GROUP BY path
),
enhanced_docs AS (
    -- Transform elements array to replace figure descriptions with VLM responses
    SELECT 
        p.path,
        p.parsed_data as original_parsed_data,
        COALESCE(v.vlm_desc_map, MAP()) as vlm_desc_map,
        -- Transform each element: replace description for figures with VLM enhancement
        TRANSFORM(
            CAST(p.parsed_data:document:elements AS ARRAY<VARIANT>),
            element -> CASE
                WHEN CAST(element:type AS STRING) = 'figure' 
                     AND v.vlm_desc_map IS NOT NULL 
                     AND v.vlm_desc_map[CAST(element:id AS STRING)] IS NOT NULL
                THEN 
                    -- Use PARSE_JSON with TO_JSON to ensure proper VARIANT conversion
                    PARSE_JSON(
                        TO_JSON(
                            NAMED_STRUCT(
                                'id', element:id,
                                'type', element:type,
                                'bbox', element:bbox,
                                'description', v.vlm_desc_map[CAST(element:id AS STRING)],
                                'content', element:content
                            )
                        )
                    )
                ELSE element  -- Keep original VARIANT for non-figures or non-enhanced figures
            END
        ) as enhanced_elements
    FROM IDENTIFIER(:catalog || '.' || :schema || '.silver_parsed_documents') p
    LEFT JOIN vlm_map v ON p.path = v.path
)
SELECT 
    e.path,
    e.original_parsed_data,
    COALESCE(f.vlm_enhanced_figures, ARRAY()) as vlm_enhanced_figures,
    -- Reconstruct complete document structure with enhanced elements
    PARSE_JSON(
        TO_JSON(
            NAMED_STRUCT(
                'document', NAMED_STRUCT(
                    'elements', e.enhanced_elements,
                    'pages', e.original_parsed_data:document:pages
                ),
                'metadata', e.original_parsed_data:metadata
            )
        )
    ) as vlm_enhanced_parsed_data
FROM enhanced_docs e
LEFT JOIN vlm_figures_agg f ON e.path = f.path;


-- Display summary results
SELECT 
    COUNT(*) as total_documents,
    SUM(CASE WHEN vlm_enhanced_figures IS NOT NULL THEN SIZE(vlm_enhanced_figures) ELSE 0 END) as total_enhanced_figures
FROM IDENTIFIER(:catalog || '.' || :schema || '.final_documents');