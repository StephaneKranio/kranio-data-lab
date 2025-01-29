from pyspark.sql import SparkSession
from google.cloud import storage
from pyspark.sql.functions import *
import logging

# Inicializar Spark
spark = SparkSession.builder.appName("CreateFinalTable").getOrCreate()

# Función para leer todos los archivos Parquet desde GCS
def read_all_parquet_files(file_paths):
    """
    Lee archivos Parquet desde una lista de rutas y los devuelve como DataFrames.
    """
    return spark.read.parquet(file_paths)

# Función para combinar y eliminar duplicados en los DataFrames
def union_and_deduplicate(dfs):
    """
    Combina una lista de DataFrames y elimina los duplicados.
    """
    if len(dfs) == 0:
        return None
    
    combined_df = dfs[0]
    for df in dfs[1:]:
        combined_df = combined_df.union(df)
    
    # Eliminar duplicados ignorando las columnas relacionadas con la ingestión
    columns_to_ignore = ['INGESTION_DATE', 'INGESTION_YEAR', 'INGESTION_MONTH', 'INGESTION_DAY']
    deduplicated_df = combined_df.dropDuplicates([col for col in combined_df.columns if col not in columns_to_ignore])
    return deduplicated_df

# Función para recuperar todas las rutas completas de archivos desde GCS
def read_full_path(bucket_name, prefix):
    """
    Recupera todas las rutas completas de archivos en un bucket GCS bajo un prefijo específico.
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    full_paths = [f"gs://{bucket_name}/{blob.name}" for blob in blobs if blob.name.endswith('.parquet')]
    return full_paths

# Cargar y procesar los datos desde la zona de procesamiento
def load_and_process_data():
    """
    Carga los datos desde GCS y combina los archivos Parquet por cada tabla (clientes, productos, ventas, stocks).
    """
    ventes_files = read_full_path("kranio-data-lab-processing", prefix="ventes")
    stocks_files = read_full_path("kranio-data-lab-processing", prefix="stocks")
    produits_files = read_full_path("kranio-data-lab-processing", prefix="produits")
    clients_files = read_full_path("kranio-data-lab-processing", prefix="clients")

    ventes_dfs = [read_all_parquet_files(file) for file in ventes_files]
    stocks_dfs = [read_all_parquet_files(file) for file in stocks_files]
    produits_dfs = [read_all_parquet_files(file) for file in produits_files]
    clients_dfs = [read_all_parquet_files(file) for file in clients_files]

    ventes_combined = union_and_deduplicate(ventes_dfs)
    stocks_combined = union_and_deduplicate(stocks_dfs)
    produits_combined = union_and_deduplicate(produits_dfs)
    clients_combined = union_and_deduplicate(clients_dfs)

    return ventes_combined, stocks_combined, produits_combined, clients_combined

# Transformación de los datos para crear la tabla final
def transform_data(stocks, ventes, produits, clients):
    """
    Realiza las transformaciones necesarias para combinar los datos en una tabla final:
    - Renombra columnas de ingestión para evitar conflictos.
    - Realiza joins entre las tablas (ventas, productos, clientes, stocks).
    - Calcula columnas derivadas: Stock_Actuel y Chiffre_Affaires.
    """
    ventes = ventes.withColumnRenamed("Ingestion_date", "Ingestion_date_ventes")
    stocks = stocks.withColumnRenamed("Ingestion_date", "Ingestion_date_stocks")
    produits = produits.withColumnRenamed("Ingestion_date", "Ingestion_date_produits")
    clients = clients.withColumnRenamed("Ingestion_date", "Ingestion_date_clients")
    
    ventes = ventes.withColumnRenamed("Execution_date", "Execution_date_ventes")
    stocks = stocks.withColumnRenamed("Execution_date", "Execution_date_stocks")
    produits = produits.withColumnRenamed("Execution_date", "Execution_date_produits")
    clients = clients.withColumnRenamed("Execution_date", "Execution_date_clients")

    # Joins entre las tablas
    ventes_produits_df = ventes.join(produits, on=["ID_Produit", "Ingestion_day", "Ingestion_month", "Ingestion_year"], how="left")
    ventes_produits_clients_df = ventes_produits_df.join(clients, on=["ID_Client", "Ingestion_day", "Ingestion_month", "Ingestion_year"], how="left")
    table_finale_df = ventes_produits_clients_df.join(stocks, on=["ID_Produit", "Ingestion_day", "Ingestion_month", "Ingestion_year"], how="left")

    # Calcular columnas derivadas
    table_finale_df = table_finale_df.withColumn("Stock_Actuel", col("Quantite_En_Stock") - col("Quantite_Vendue"))
    table_finale_df = table_finale_df.withColumn("Chiffre_Affaires", col("Quantite_Vendue") * col("Prix_Unitaire"))

    table_finale_df.printSchema()
    return table_finale_df

# Escribir la tabla final en GCS
def write_final_table(df):
    """
    Escribe la tabla final transformada en el bucket GCS en la zona de consumo.
    """
    output_path = "gs://kranio-data-lab-consumption/tabla-perfecta"
    df.write.mode("overwrite").parquet(output_path)
    logging.info(f"Tabla final escrita en: {output_path}")

# Pipeline principal
def main():
    """
    Ejecuta el flujo completo:
    - Carga los datos desde la zona de procesamiento.
    - Realiza las transformaciones necesarias.
    - Escribe la tabla final en la zona de consumo.
    """
    logging.info("Cargando y procesando datos...")
    ventes, stocks, produits, clients = load_and_process_data()

    logging.info("Transformando los datos...")
    final_table = transform_data(stocks, ventes, produits, clients)

    logging.info("Escribiendo la tabla final en la zona de consumo...")
    write_final_table(final_table)

    logging.info("Proceso completado con éxito.")

# Ejecutar el pipeline
if __name__ == "__main__":
    main()



