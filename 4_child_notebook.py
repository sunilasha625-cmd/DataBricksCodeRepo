# Databricks notebook source
# MAGIC %md
# MAGIC #Creating this child notebook for the demo of calling child notebook from the parent notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_timestamp()

# COMMAND ----------

dbutils.widgets.text("param1","default value")
paramvalue=dbutils.widgets.get("param1")
print("param passed from the parent nb - ",paramvalue)



# COMMAND ----------

dbutils.notebook.exit(0)
