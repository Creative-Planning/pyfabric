# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00000000-0000-0000-0000-000000000001",
# META       "default_lakehouse_name": "lh_primary",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-0000000000aa",
# META       "known_lakehouses": [
# META         {
# META           "id": "00000000-0000-0000-0000-000000000001"
# META         },
# META         {
# META           "id": "00000000-0000-0000-0000-000000000002"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Example notebook
# Two markdown lines.

# CELL ********************

%pip install "builtin/example_pkg-0.1.0-py3-none-any.whl" --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("hello")
x = 1 + 2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
