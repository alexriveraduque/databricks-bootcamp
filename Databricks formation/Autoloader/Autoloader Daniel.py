# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
# 1. Definimos las variables de nuestro entorno (Unity Catalog)
catalog_name = "bronze"
schema_name = "rrhh"
table_name = "clients_brz"

# Concatenamos siguiendo el estándar de 3 niveles
full_table_name = f"{catalog_name}.{schema_name}.{table_name}"





# Ruta técnica para el manejo de estado del streaming
checkpoint_path     = f"s3://lakehouse-datawizard-dev/bronze-dev/_pipelines/{table_name}/_checkpoint/"
schema_path         = f"s3://lakehouse-datawizard-dev/bronze-dev/_pipelines/{table_name}/_schema/"
landing_path        = "s3://lakehouse-datawizard-dev/landing-dev/clients/"

print(full_table_name)

# COMMAND ----------


df_test = (
    spark.readStream
    .format("cloudFiles")
    # =========================================================================
    # 1. CONFIGURACIÓN BASE
    # =========================================================================
    .option("cloudFiles.format", "csv") # Requerido: Formato origen (csv, json, parquet, avro)
    # =========================================================================
    # 2. GESTIÓN DE ESQUEMA Y TIPOS DE DATOS
    # =========================================================================
    # Ruta obligatoria si usas inferencia o evolución de esquema. 
    # Aquí Auto Loader guarda el esquema detectado para no recalcularlo.
    .option("cloudFiles.schemaLocation", schema_path)
        # Opción A: Inferencia Automática
    # [POR DEFECTO: "false"] -> Si es false, lee todo como STRING (ideal para capa Bronze).
    # .option("cloudFiles.inferColumnTypes", "false") 

        # Opción B: Forzar un tipo de dato específico (Hints)
    # Útil si inferColumnTypes="true" pero quieres corregir una columna que Spark suele adivinar mal.
    # .option("cloudFiles.schemaHints", "venta_id INT, birth_date DATE")
    
        # Opción C: Proveer el esquema manual (Hardcoded)
    # Si usas esto, Auto Loader es más rápido, pero pierdes la evolución automática de esquema.
    # .schema("id STRING, name STRING, last_name STRING, email STRING, birth_date STRING, country STRING, role STRING")

    # =========================================================================
    # 3. OPCIONES ESPECÍFICAS DEL FORMATO (Solo para CSV)
    # =========================================================================
    # [POR DEFECTO: "true"] -> Le indica a Spark si la primera fila tiene los nombres de las columnas.
    # En producción con CSVs estructurados, casi siempre lo cambiamos a "true".
    # .option("header", "false") 
    # .option("sep", ",") # [POR DEFECTO: ","] -> Puedes cambiar el delimitador si usas pipes (|) o punto y coma (;)

    # =========================================================================
    # 4. EVOLUCIÓN DE ESQUEMA (¿Qué pasa si llega una columna nueva?)
    # =========================================================================
    # [POR DEFECTO: "addNewColumns"] -> Agrega la columna nueva al DataFrame automáticamente.
    # .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    # Alternativas de evolución (Descomentar solo una según la regla de negocio):
    .option("cloudFiles.schemaEvolutionMode", "rescue") # Ignora la columna nueva y la mete entera como JSON en `_rescued_data`.
    # .option("cloudFiles.schemaEvolutionMode", "failOnNewColumns") # Falla el job inmediatamente (útil para contratos de datos estrictos).
    # .option("cloudFiles.schemaEvolutionMode", "none") # Ignora la columna nueva por completo y no la lee.

    # =========================================================================
    # 5. CONTROL DE FLUJO Y RENDIMIENTO (Throughput)
    # =========================================================================
    # [POR DEFECTO: "1000"] -> Límite máximo de archivos físicos a procesar en cada micro-batch.
    # Bajarlo (ej. a 100) protege al cluster de quedarse sin memoria si hay un atraso masivo de archivos pequeños.
    # .option("cloudFiles.maxFilesPerTrigger", "100")
    # [POR DEFECTO: No tiene] -> Límite por tamaño en bytes.
    # Es más seguro que maxFiles si tus archivos varían mucho de tamaño. (100MB = 100000000)
    # .option("cloudFiles.maxBytesPerTrigger", "100m") # Tip: Puedes usar "100m" o "1g" para mayor legibilidad.

    # =========================================================================
    # LECTURA FINAL
    # =========================================================================
    .load(landing_path)
)

# COMMAND ----------

# MAGIC  %skip
# MAGIC checkpoint_path  = f"s3://lakehouse-datawizard-dev/bronze-dev/_pipelines/{table_name}/_checkpoint/display/13"
# MAGIC display(df_test, checkpointLocation = checkpoint_path)

# COMMAND ----------

df_bronze_raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("header", "true")
    .load(landing_path)
)

# COMMAND ----------


query_production = (
    df_test
    .writeStream
    .format("delta")                               # Ya no es 'memory', ahora es persistente
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path) # Obligatorio al escribir en Delta
    .option("mergeSchema", "true") # <--- Clave para Delta Lake
    .trigger(availableNow=True)                    # Procesa lo pendiente y termina
    .toTable(full_table_name)                      # <--- AQUÍ DEFINES catalogo.esquema.tabla
)

# COMMAND ----------

df= spark.read.table(full_table_name)
display(df)

# COMMAND ----------


