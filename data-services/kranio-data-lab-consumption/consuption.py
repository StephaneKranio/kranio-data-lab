from pyspark.sql import SparkSession
from google.cloud import storage
from pyspark.sql.functions import *

# Initialize Spark
spark = SparkSession.builder.appName("TransformSalesAndStocks").getOrCreate()

# Fonction pour lire tous les fichiers Parquet depuis GCS
def read_all_parquet_files(file_paths):
    return spark.read.parquet(file_paths)

# Fonction pour combiner et dédupliquer les DataFrames
def union_and_deduplicate(dfs):
    if len(dfs) == 0:
        return None
    
    combined_df = dfs[0]
    for df in dfs[1:]:
        combined_df = combined_df.union(df)
    
    columns_to_ignore = ['ingestion_date', 'year', 'month', 'day']
    deduplicated_df = combined_df.dropDuplicates([col for col in combined_df.columns if col not in columns_to_ignore])
    return combined_df

# Fonction pour récupérer tous les chemins de fichiers depuis GCS
def read_full_path(bucket_name, prefix):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    full_paths = [f"gs://{bucket_name}/{blob.name}" for blob in blobs if blob.name.endswith('.parquet')]
    return full_paths

# Charger et traiter les données
def load_and_process_data():
    ventes_files = read_full_path("simplon-dev-processing-sr", prefix="silver/ventes_jeudi")
    stocks_files = read_full_path("simplon-dev-processing-sr", prefix="silver/stocks_jeudi")
    produits_files = read_full_path("simplon-dev-processing-sr", prefix="silver/produits")
    clients_files = read_full_path("simplon-dev-processing-sr", prefix="silver/clients")

    ventes_dfs = [read_all_parquet_files(file) for file in ventes_files]
    stocks_dfs = [read_all_parquet_files(file) for file in stocks_files]
    produits_dfs = [read_all_parquet_files(file) for file in produits_files]
    clients_dfs = [read_all_parquet_files(file) for file in clients_files]

    ventes_combined = union_and_deduplicate(ventes_dfs)
    stocks_combined = union_and_deduplicate(stocks_dfs)
    produits_combined = union_and_deduplicate(produits_dfs)
    clients_combined = union_and_deduplicate(clients_dfs)

    return ventes_combined, stocks_combined, produits_combined, clients_combined

# Transformation des données
def transform_data(stocks, ventes, produits, clients):
    ventes = ventes.withColumnRenamed("ingestion_date", "ingestion_date_ventes")
    stocks = stocks.withColumnRenamed("ingestion_date", "ingestion_date_stocks")
    produits = produits.withColumnRenamed("ingestion_date", "ingestion_date_produits")
    clients = clients.withColumnRenamed("ingestion_date", "ingestion_date_clients")

    ventes_produits_df = ventes.join(produits, on=["ID_Produit", "année", "mois", "jour"], how="left")
    ventes_produits_clients_df = ventes_produits_df.join(clients, on=["ID_Client", "année", "mois", "jour"], how="left")
    table_finale_df = ventes_produits_clients_df.join(stocks, on=["ID_Produit", "année", "mois", "jour"], how="left")

    table_finale_df = table_finale_df.withColumn("Stock_Actuel", col("Quantité_En_Stock") - col("Quantité_Vendue"))
    table_finale_df = table_finale_df.withColumn("Chiffre_Affaires", col("Quantité_Vendue") * col("Prix_Unitaire"))

    table_finale_df.printSchema()
    return table_finale_df
