
## Wrap an `[KIE]agent` in a MLflow Custom Pyfunc for Custom Evals

**Use case:** Users may require custom evaluation metrics—such as external LLMs, proprietary calculations, or in-house evaluation methods—to assess the Agent Bricks Information Extraction Agent and more, beyond the default LLM-as-a-judge options provided by Databricks.

**`mlflow_pyfunc_wrapped_kieAgent` is strictly a reference example notebook** that wraps a [Databricks Agent Bricks - Information Extraction Agent](https://docs.databricks.com/aws/en/generative-ai/agent-bricks/key-info-extraction) (`beta`) endpoint as a [MLflow Custom Pyfunc](https://mlflow.org/blog/custom-pyfunc) which allows for e.g. downstream [MLflow GenAI Scorer/Judge evaluations](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/predefined-judge-scorers) where needed. This approach technically applies also other '`Agents`' (not specific to Agent Bricks).

<br>

---    

**Requirements:**

- **Minimum DBR required**: `MLdbr16.4LTS_scala2.13 {"spark_version": "16.4.x-scala2.13", "node_type_id": "Standard_D16ds_v5"}` 

- **IMPORTANT**: An `KIE` agent endpoint url -- one that is created via [AgentBricks](https://www.databricks.com/product/artificial-intelligence/agent-bricks) e.g. "`https://{databricks_workspace_url}/serving-endpoints/{kie}-########-endpoint/invocations`"

- **NOTE**: A sample of de-identified data on device related repair and maintenance workorders for illustration is used in this notebook. The `KIE` Agent you developed will have a different response output with actual input data -- use the example notebook as a reference for your use case and use your own sample data where possible. 

<br>

---    

##### Dependencies used in the reference notebook and their associated licences:  

| Package                | Version Spec      | License         | Reference Link                                                                 |
|------------------------|------------------|-----------------|-------------------------------------------------------------------------------|
| mlflow                 | 3.1.4            | Apache 2.0      | [MLflow License](https://github.com/mlflow/mlflow/blob/master/LICENSE.txt)     |
| databricks-agents      | 1.2.0            | Apache 2.0      | [databricks-agents License](https://pypi.org/project/databricks-agents/) |
| cloudpickle            | >=3.1.1          | BSD 3-Clause    | [cloudpickle License](https://github.com/cloudpipe/cloudpickle/blob/master/LICENSE)     |


<br> 

---    

**REFs:**
- https://docs.databricks.com/aws/en/generative-ai/agent-framework/author-agent#what-if-i-already-have-an-agent
- https://mlflow.org/docs/latest/api_reference/python_api/mlflow.pyfunc.html#mlflow.pyfunc.ResponsesAgent