
## Wrap an `[KIE]agent` in a Custom Pyfunc + Explore Custom Evals

[### nb_name] reference example notebook that wraps a [KIE Agent](https://docs.databricks.com/aws/en/generative-ai/agent-bricks/key-info-extraction) (`beta`) endpoint as a [MLflow Custom Pyfunc](https://mlflow.org/blog/custom-pyfunc) which allows for e.g. downstream [MLflow Genai Scorer/Judge evaluations](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/predefined-judge-scorers) where needed. This approach technically applies also other '`Agents`' (not specific to Agent Bricks).

**Requirements:**

- Minimum DBR required: MLdbr16.4LTS_scala2.13 {"spark_version": "16.4.x-scala2.13", "node_type_id": "Standard_D16ds_v5"} 

- An {KIE} agent endpoint url: one that is created via [AgentBricks](https://www.databricks.com/product/artificial-intelligence/agent-bricks) e.g. "`https://{databricks_workspace_url}/serving-endpoints/{kie}-########-endpoint/invocations`"

- A sample of de-identified data on device related repair and maintenance workorders for illustration is used in this notebook. 


---    

**REFs:**
- https://docs.databricks.com/aws/en/generative-ai/agent-framework/author-agent#what-if-i-already-have-an-agent
- https://mlflow.org/docs/latest/api_reference/python_api/mlflow.pyfunc.html#mlflow.pyfunc.ResponsesAgent