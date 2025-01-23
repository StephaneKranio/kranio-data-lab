from pyspark.sql.functions import *
from pyspark.sql.types import StringType, FloatType, IntegerType
from datetime import datetime
from pyspark.sql import SparkSession
# Initialize Spark
spark = SparkSession.builder.appName("TransformSalesAndStocks").getOrCreate()

# Fonction pour caster les colonnes
def cast_columns(df):
    for column_name in df.columns:
        df = df.withColumn(column_name, trim(col(column_name)))  # Enlever les espaces
        
        if 'date' in column_name.lower():
            df = df.withColumn(column_name, regexp_replace(col(column_name), '[^0-9-]', ''))  # Garder les chiffres et '-'
            df = df.withColumn(column_name, to_date(col(column_name), 'dd-MM-yyyy'))
        elif 'prix' in column_name.lower() or 'montant' in column_name.lower():
            df = df.withColumn(column_name, regexp_replace(col(column_name), '[^0-9.]', ''))
            df = df.withColumn(column_name, col(column_name).cast(FloatType()))
        elif 'quantité' in column_name.lower() or 'nombre' in column_name.lower():
            df = df.withColumn(column_name, regexp_replace(col(column_name), '[^0-9]', ''))
            df = df.withColumn(column_name, col(column_name).cast(IntegerType()))
    
    return df

# Fonction pour nettoyer les colonnes String
def clean_string_columns(df):
    for column_name in df.columns:
        if df.schema[column_name].dataType == StringType():
            df = df.withColumn(column_name, upper(col(column_name)))
            df = df.withColumn(column_name, trim(col(column_name)))
    return df

# Fonction pour ajouter des colonnes d'ingestion
def add_ingestion_columns(df):
    current_date_ingestion = datetime.now()
    df = df.withColumn('ingestion_date', lit(current_date_ingestion.strftime('%Y-%m-%d')))
    df = df.withColumn('année', lit(current_date_ingestion.year))
    df = df.withColumn('mois', lit(current_date_ingestion.month))
    df = df.withColumn('jour', lit(current_date_ingestion.day))
    return df

# Fonction pour traiter les valeurs nulles
def fill_null_values(df):
    df = df.na.fill({column_name: '' for column_name in df.columns if df.schema[column_name].dataType == StringType()})
    return df

# Fonction principale pour la transformation
def transform_table(df):
    df = cast_columns(df)
    df = clean_string_columns(df)
    df = add_ingestion_columns(df)
    df = fill_null_values(df)
    df = df.dropDuplicates()
    return df

# Fonction pour écrire les données dans GCS (zone silver)
def write_to_silver(df, table_name, bucket_name, path):
    current_date_ingestion = datetime.now()
    year = current_date_ingestion.year
    month = current_date_ingestion.month
    day = current_date_ingestion.day
    df.write.parquet(f"gs://{bucket_name}/{path}/{table_name}/{year}/{month}/{day}", mode="overwrite")

# Fonction principale pour traiter les tables
def process_tables(source_files, bucket_name, path):
    for table_name, source_file in source_files.items():
        df = spark.read.parquet(f"gs://{bucket_name}/{source_file}")
        df_transformed = transform_table(df)
        write_to_silver(df_transformed, table_name, bucket_name, path)
    print("Transformation des tables de bronze à silver terminée.")

# Utilisation
source_files = {
    'clients': "path_to_clients_in_bronze",
    'produits': "path_to_produits_in_bronze",
    'stocks_jeudi': "path_to_stocks_in_bronze",
    'ventes_jeudi': "path_to_ventes_in_bronze"
}

process_tables(source_files, "simplon-dev-processing-sr", "silver")
