import argparse
import logging
import unicodedata
import re
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Initialiser Spark et configuration du logger
spark = SparkSession.builder.appName('ingestion-raw').getOrCreate()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# Métadonnées d'ingestion
ingestion_metadata = {
    'clients': {
        'input_file': 'clients.csv',
        'output_path': 'clients',
        'sep': ',',
        'encoding': 'UTF-8',
    },
    'produits': {
        'input_file': 'produits.csv',
        'output_path': 'produits',
        'sep': ',',
        'encoding': 'UTF-8',
    },
    'stocks': {
        'input_file': 'stocks.csv',
        'output_path': 'stocks',
        'sep': ',',
        'encoding': 'UTF-8',
    },
    'ventes': {
        'input_file': 'ventes.csv',
        'output_path': 'ventes',
        'sep': ',',
        'encoding': 'UTF-8',
    },
}

def handle_arguments():
    """Gérer les arguments en ligne de commande."""
    parser = argparse.ArgumentParser(description="Script de ingestion para Dataproc.")
    parser.add_argument('--bucket', required=False, default='kranio-data-lab-ingestion', help="Nom du bucket GCS")
    parser.add_argument('--stage', required=False, default='dev', help='Stage environment (e.g., dev, prod)')
    parser.add_argument('--ingestion_date', required=True, help="Fecha de ingestion (format YYYY-MM-DD)")
    parser.add_argument('--execution_date', required=True, help="Fecha de ingestion (format YYYY-MM-DD)")
    return parser.parse_args()

def remove_accents(input_str):
    """Eliminar acentos de una cadena de texto."""
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])


def clean_column_name(col_name):
    """Nettoyer un nom de colonne : supprimer les accents, mettre en majuscules, supprimer les caractères spéciaux."""
    col_name = remove_accents(col_name)   # Supprimer les accents
    col_name = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)  # Remplacer les caractères non alphanumériques par _
    col_name = col_name.upper()  # Convertir en majuscules
    col_name = re.sub(r'__+', '_', col_name).strip('_')  # Éliminer les doubles underscores et nettoyer les bords
    return col_name

def clean_dataframe(df):
    """Appliquer les transformations de base sur un DataFrame Spark."""
    # Nettoyer les noms de colonnes
    df = df.toDF(*[clean_column_name(col) for col in df.columns])
    
    # Exemple supplémentaire de transformation : supprimer les espaces autour des chaînes de caractères
    for col in df.columns:
        df = df.withColumn(col, F.trim(F.col(col)))
    
    return df

def process_table(input_path, output_path, sep, encoding, ingestion_date, execution_date):
    """Lire, transformer et écrire les données pour une table."""
    # Lecture des données
    logging.info(f"Lecture des données depuis {input_path}")
    df = spark.read.csv(input_path, header=True, sep=sep, encoding=encoding, inferSchema=True)

    # Nettoyer les noms de colonnes et appliquer les transformations de base
    logging.info("Nettoyage des colonnes et transformation des données.")
    df = clean_dataframe(df)

    # Ajout des colonnes d'ingestion et d'exécution
    logging.info("Ajout des colonnes d'ingestion et d'exécution.")
    df = (df
          .withColumn('INGESTION_DATE', F.lit(ingestion_date))
          .withColumn('EXECUTION_DATE', F.lit(execution_date))
          .withColumn('INGESTION_YEAR', F.year(F.lit(ingestion_date)))
          .withColumn('INGESTION_MONTH', F.month(F.lit(ingestion_date)))
          .withColumn('INGESTION_DAY', F.dayofmonth(F.lit(ingestion_date))))

    # Sauvegarde des données
    logging.info(f"Écriture des données transformées dans {output_path}")
    (df.write
     .partitionBy('INGESTION_YEAR', 'INGESTION_MONTH', 'INGESTION_DAY')
     .mode('overwrite')
     .parquet(output_path))

def main():
    args = handle_arguments()
    bucket = args.bucket
    ingestion_date = args.ingestion_date
    execution_date = args.execution_date

    # Préfixes pour les chemins GCS
    csv_input_prefix = f'gs://{bucket}/landing-zone'
    output_prefix = f'gs://{bucket}/raw-zone'

    # Processus d'ingestion pour chaque table
    for table_name, params in ingestion_metadata.items():
        logging.info(f"Traitement de la table {table_name}...")
        input_file = params['input_file']
        output_path = params['output_path']
        sep = params['sep']
        encoding = params['encoding']

        # Chemins complets
        full_input_path = f'{csv_input_prefix}/{input_file}'
        full_output_path = f'{output_prefix}/{output_path}'

        # Traitement des données
        process_table(full_input_path, full_output_path, sep, encoding, ingestion_date, execution_date)

if __name__ == "__main__":
    main()


