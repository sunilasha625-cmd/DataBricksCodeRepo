# Databricks notebook source
# MAGIC %md
# MAGIC #####1. Display the list databricks utils

# COMMAND ----------

# MAGIC %md
# MAGIC ######Below dbutils is the comprehensive one, out of which we are going to concentrate currently on notebook, widgets and fs for now

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Notebook utils help

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Widgets utils help

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("team_name","Enter team name","This is to represent our team name")

# COMMAND ----------

text_box_value1=dbutils.widgets.get("team_name")
print("Good Morning ",text_box_value1)

# COMMAND ----------

dbutils.widgets.dropdown("listbox","wd36",["wd32","we43","we45","wd36"],"Team names drop down")
listbox_value2=dbutils.widgets.get("listbox")
print("Good morning",listbox_value2)

# COMMAND ----------

dbutils.widgets.combobox("combobox","we47",["wd32","we43","we45","we47"],"Team names combo box")

# COMMAND ----------

dbutils.widgets.multiselect("multiselect","wd36",["wd32","we43","we45","wd36"],"Team names multiselect")

# COMMAND ----------

dict_all_widgets=dbutils.widgets.getAll()
print(dict_all_widgets)

# COMMAND ----------

# MAGIC %md
# MAGIC #####4. Calling a child notebook (example_child_notebook.ipynb) from this parent notebook with parameters
# MAGIC dbutils.widgets.text("param1", "default_value", "Your input parameter")
# MAGIC param_value = dbutils.widgets.get("param1")
# MAGIC print("printing the parameters",param_value)

# COMMAND ----------

child_return_value=dbutils.notebook.run("/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/databricks_workouts_2025/1_DATABRICKS_NOTEBOOK_FUNDAMENTALS/4_child_notebook", 180,{"param1":text_box_value1})

# COMMAND ----------

dbutils.widgets.removeAll()
