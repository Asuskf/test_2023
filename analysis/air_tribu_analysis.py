# Databricks notebook source
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

# MAGIC %md 
# MAGIC ## Answers

# COMMAND ----------

def dataFlights(filter_by : str, top = 5, order_by = "DESC"):
    query = f"""
    SELECT
        pais,
        COUNT(pais) as num_vuelos
    FROM
        flights
    INNER JOIN dict_countries
        ON flights.{filter_by} = dict_countries.COD_pais
    GROUP BY
        pais
    ORDER BY 
        num_vuelos {order_by}
    LIMIT
        {top}
    """
    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ### ¿De que país salieron más aviones?

# COMMAND ----------

query = dataFlights('origen', top = 5)
destination_flights = spark.sql(query)
saveTable(destination_flights, "more_planes_left")
display(destination_flights)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ¿A que país llegaron más aviones?

# COMMAND ----------

query = dataFlights('destino', top = 5)
origin_flights = spark.sql(query)
saveTable(origin_flights, "more_planes_arrived")
display(origin_flights)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ¿Qué día hubo más vuelos? ¿y menos?

# COMMAND ----------

def moreLessFlights(top = 5, order_by = 'DESC'):    
    query = f"""
    SELECT
        dia,
        COUNT(dia) as num_vuelos
    FROM
        dates
    INNER JOIN flights
        ON dates.vuelo = flights.vuelo
    GROUP BY 
        dia
    ORDER BY
        num_vuelos {order_by}
    LIMIT
        {top}
    """
    return query

# COMMAND ----------

# MAGIC %md
# MAGIC #### más vuelos

# COMMAND ----------

query = moreLessFlights()
more_flights = spark.sql(query)
saveTable(more_flights, "more_flights")
display(more_flights)

# COMMAND ----------

# MAGIC %md
# MAGIC #### menos vuelos

# COMMAND ----------

query = moreLessFlights(order_by="ASC")
less_flights = spark.sql(query)
saveTable(less_flights, "less_flights")
display(less_flights)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### ¿Qué día hubo más retrasos? ¿y menos?

# COMMAND ----------

def moreLessDelay(top = 5, order_by = 'DESC'):
    query = f"""
    SELECT
        dia,
        SUM(retraso) as suma_retraso
    FROM
        dates
    INNER JOIN flights 
        ON dates.vuelo = flights.vuelo
    INNER JOIN delays
        ON flights.vuelo = delays.vuelo
    GROUP BY 
        dia
    ORDER BY
        suma_retraso {order_by}
    LIMIT
        {top}
    """
    return query

# COMMAND ----------

# MAGIC %md
# MAGIC #### más retrasos

# COMMAND ----------

query = moreLessDelay()
more_delay = spark.sql(query)
saveTable(more_delay, "more_delay")
display(more_delay)

# COMMAND ----------

# MAGIC %md
# MAGIC #### día menos retraso

# COMMAND ----------

query = moreLessDelay(order_by="ASC")
less_delay = spark.sql(query)
saveTable(less_delay, "less_delay")
display(less_delay)
