# Databricks notebook source
# MAGIC %md
# MAGIC # Causilo on a Databricks table
# MAGIC
# MAGIC Fills the blank cells of one column. The filled rows are the examples, the blank
# MAGIC rows are predicted, and the predictions are saved as a table next to the source.
# MAGIC Running it again needs another `output_table`, or `mode="overwrite"` below.
# MAGIC
# MAGIC The cluster needs outbound HTTPS to `api.nums.world`. The key is read from a
# MAGIC secret scope, never typed into the notebook:
# MAGIC
# MAGIC ```
# MAGIC databricks secrets create-scope causilo
# MAGIC databricks secrets put-secret causilo api_key
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install causilo-db
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("table", "main.default.customers")
dbutils.widgets.text("target", "churn")
dbutils.widgets.text("id_column", "customer_id")
dbutils.widgets.text("output_table", "main.default.customers_churn_prediction")

# COMMAND ----------

from causilo_db.spark import predict_table

out = predict_table(
    spark,
    dbutils.widgets.get("table"),
    dbutils.widgets.get("target"),
    id_column=dbutils.widgets.get("id_column") or None,
    output_table=dbutils.widgets.get("output_table") or None,
    api_key=dbutils.secrets.get("causilo", "api_key"),
    mode="error",  # or "overwrite" / "append" when output_table already exists
)
display(out)
