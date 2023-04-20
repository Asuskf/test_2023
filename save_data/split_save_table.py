# Databricks notebook source
# MAGIC %md
# MAGIC ## 0.- Install and import required dependencies

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install libraries

# COMMAND ----------

# MAGIC %pip install azure-storage-blob openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import libraries

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.- Development

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions

# COMMAND ----------

def createDataframe(df: pd.DataFrame, index: int, aux_index: int):
    """
    Create the dataframes using the columns without data as breakpoints
        param:
            - df: All dataframe
            - index: The index of the column that starts the dataframe
            - aux_index: The index of the column that ends the dataframe
        return:
            aux_df = A pandas dataframe 
    """
    aux_df = df.iloc[:, aux_index:empty_columns[index]].dropna().reset_index(drop=True)
    return aux_df

# COMMAND ----------

def createParquet(name: str, aux_df):
    """
    Saves the spark dataframe in a pandas-like file. It is a good practice to generate dimensions
        param:
            - name: The name of the parquet file
            - aux_df: The spark dataframe to save
        return:
            - None
    """
    blob_client = container_client.get_blob_client(f"{name}/{name}_3.parquet")
    output = aux_df.to_parquet()
    blob_client.upload_blob(output)

# COMMAND ----------

def createSparkDataframe(df: pd.DataFrame):
    """
    Transform pandas dataframe to spark dataframe
        param:
            - df: Pandas dataframe you need to transform
    return:
        - A spark to dataframe
    """
    spark_dataframe = spark.createDataFrame(df)
    return spark_dataframe    

# COMMAND ----------

def saveTable(spark_dataframe, table_name: str, temp_table=""):
    """
    Save spark dataframe in delta table, parquet and temporary table
        param:
            - spark_dataframe: Spark dataframe you need to transform
            - table_name: Name to save the table
            - temp_table: Name to save the temporary table by 
                          default is saved with the name of the table
        return:
            - None
    """
    if temp_table == "":
        temp_table = table_name
    spark_dataframe.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

#enter credentials
account_name = 'prediqt'
account_key = dbutils.secrets.get(scope="keys_container", key="accountkey")
container_name = 'airtribu'
file_name = '2. Air Tribu.xlsx'
#create a client to interact with blob storage
connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

#use the client to connect to the container
container_client = blob_service_client.get_container_client(container_name)

# COMMAND ----------

#generate a shared access signiture for files and load them into Python
sas_i = generate_blob_sas(account_name = account_name,
                            container_name = container_name,
                            blob_name = file_name,
                            account_key=account_key,
                            permission=BlobSasPermissions(read=True),
                            expiry=datetime.utcnow() + timedelta(hours=1))

sas_url = 'https://' + account_name+'.blob.core.windows.net/' + container_name + '/' + file_name.replace(' ',"%20") + '?' + sas_i

# COMMAND ----------

#dict with name for delta table (key) and folder name (value)
LIST_COUNTRIES = {'fecha': 'dates', 'paises': 'dict_countries', 'vuelos': 'flights', 'retrasos': 'delays'}
df_all_data = pd.read_excel(sas_url)
columns = [column_name.replace('í', 'i').replace(' ','') if isinstance(column_name, str) else column_name for column_name in df_all_data.iloc[1].tolist() ]
df_all_data.columns = columns
df_all_data = df_all_data[2:]
empty_columns = [index for index in range(0, len(columns)) if str(columns[index]) == 'nan']
empty_columns.append(len(df_all_data))

# COMMAND ----------

aux_index = 0
for index in range(0, len(empty_columns)):
    keys = list(LIST_COUNTRIES.keys())
    values = list(LIST_COUNTRIES.values())
    aux_df = createDataframe(df_all_data, index, aux_index)
    #createParquet(values[index], aux_df) #Best practice
    aux_spark_df = createSparkDataframe(aux_df)
    saveTable(aux_spark_df, values[index])
    aux_index = empty_columns[index] + 1
