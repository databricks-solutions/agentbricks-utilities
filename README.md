# AgentBricks Utilities

Includes utilities and plugins extending AgentBricks and related features such as Databricks AI functions.

The goal of utilties in this repo is to improve gaps in the product, specifically focusing on product-market fit, filling perceived gaps as perceived by customers or the field, or addressing challenges that customers face during POCs or other engagements.

The primary goal of this repo is not example implementations, though sometimes examples that direct customers more strongly toward approaches that integrate better with customer deployments such as functioning streaming or incremental code would absolutely be included since that is more of a product-market fit issue. As features get rolled into the product and each utility becomes less useful we’ll remove features and/or archive the repository.
## 

## Installation

Generally the utilities here can be copy-pasted from a notebook or the repo simply cloned for their use. If a project requires a DAB deploy or other implementation it will say so in the project-specific readme.

## Utilities

1. **PDF Profiler** - Profile and trim large PDFs to reduce processing costs. If you only need the first N pages of multi-page PDFs, this streaming-based tool extracts metadata and trims PDFs at scale before processing with `ai_parse_document()`.

2. **PDF Splitter** - Split large PDFs into smaller chunks. For example, split a 6000-page PDF into six 1000-page files, with page ranges in the filenames. Uses structured streaming for scalable processing.

3. **AI Parse Document Pipeline** - End-to-end document processing pipeline using `ai_parse_document()` and `ai_query()`. Incrementally parses PDFs and images, then extracts structured data with streaming for production-ready document ingestion.

4. **Custom Evaluation Metrics** - Reference notebooks for custom evaluation metrics using external LLMs, proprietary calculations, or in-house methods to assess Agent Bricks Information Extraction Agent beyond default LLM-as-a-judge options.

## How to get help

Databricks support doesn't cover this content. For questions or bugs, please open a GitHub issue and the team will help on a best effort basis.


## License

&copy; 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| pypdf==6.0.0 | Opens and processes PDFs | BSD-3 Clause | https://pypi.org/project/pypdf/ |
| mlflow	3.1.4	| MLflow  | Apache 2.0 | 
| databricks-agents	1.2.0	| databricks-agents | Apache 2.0 | 
| cloudpickle	>=3.1.1	| cloudpickle License | BSD 3-Clause	| 
