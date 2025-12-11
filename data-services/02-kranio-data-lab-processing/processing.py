from pyspark.sql.functions import *
from pyspark.sql.types import StringType, FloatType, IntegerType
from datetime import datetime
from pyspark.sql import SparkSession
import logging

# Configuración del logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s")

# Inicializar Spark
spark = SparkSession.builder.appName("TransformAndConcatTables").getOrCreate()

# Función para convertir columnas a tipos específicos
def cast_columns(df):
    """
    Convierte las columnas del DataFrame a tipos específicos y limpia datos básicos.
    - Elimina espacios.
    - Convierte fechas al formato correcto.
    - Convierte columnas de precios y montos a Float.
    - Convierte columnas de cantidades a Integer.
    """
    for column_name in df.columns:
        df = df.withColumn(column_name, trim(col(column_name)))  # Eliminar espacios
        
        if 'date' in column_name.lower():
            df = df.withColumn(column_name, regexp_replace(col(column_name), '[^0-9-]', ''))  # Mantener números y '-'
            df = df.withColumn(column_name, to_date(col(column_name), 'dd-MM-yyyy'))
        elif 'prix' in column_name.lower() or 'montant' in column_name.lower():
            df = df.withColumn(column_name, regexp_replace(col(column_name), '[^0-9.]', ''))
            df = df.withColumn(column_name, col(column_name).cast(FloatType()))
        elif 'quantite' in column_name.lower() or 'nombre' in column_name.lower():
            df = df.withColumn(column_name, regexp_replace(col(column_name), '[^0-9]', ''))
            df = df.withColumn(column_name, col(column_name).cast(IntegerType()))
    
    return df

# Función para limpiar columnas de tipo String
def clean_string_columns(df):
    """
    Limpia las columnas String del DataFrame.
    - Convierte los textos a mayúsculas.
    - Elimina espacios al inicio y al final.
    """
    for column_name in df.columns:
        if df.schema[column_name].dataType == StringType():
            df = df.withColumn(column_name, upper(col(column_name)))
            df = df.withColumn(column_name, trim(col(column_name)))
    return df

# Función para agregar columnas de metadata de ingestión
def add_ingestion_columns(df):
    """
    Agrega columnas relacionadas con la ingestión:
    - Fecha de ingestión.
    - Año, mes y día de ingestión.
    """
    current_date_ingestion = datetime.now()
    df = df.withColumn('Ingestion_date', lit(current_date_ingestion.strftime('%Y-%m-%d')))
    df = df.withColumn('Ingestion_year', lit(current_date_ingestion.year))
    df = df.withColumn('Ingestion_month', lit(current_date_ingestion.month))
    df = df.withColumn('Ingestion_day', lit(current_date_ingestion.day))
    return df

# Función para rellenar valores nulos
def fill_null_values(df):
    """
    Rellena valores nulos en columnas de tipo String con cadenas vacías.
    """
    df = df.na.fill({column_name: '' for column_name in df.columns if df.schema[column_name].dataType == StringType()})
    return df

# Función principal para transformar las tablas
def transform_table(df):
    """
    Aplica una serie de transformaciones al DataFrame:
    - Limpieza de columnas.
    - Conversión de tipos de datos.
    - Agregación de columnas de metadata.
    - Eliminación de duplicados.
    """
    df = cast_columns(df)
    df = clean_string_columns(df)
    df = add_ingestion_columns(df)
    df = fill_null_values(df)
    df = df.dropDuplicates()
    return df

# Función para escribir las tablas transformadas en la zona de procesamiento (processing)
def write_to_processing(df, table_name, bucket_name, path):
    """
    Escribe las tablas transformadas en el bucket de la zona de procesamiento (processing).
    """
    df.write.mode("append").parquet(f"gs://{bucket_name}/{path}/{table_name}")

# Función principal para procesar y concatenar tablas
def process_and_concat_tables(raw_bucket, processing_bucket, raw_path, processing_path):
    """
    Procesa y concatena tablas de la zona raw-zone y las escribe en processing.
    """
    # Definir las rutas de origen para las tablas
    table_paths = {
        'clients': f"{raw_bucket}/{raw_path}/clients",
        'produits': f"{raw_bucket}/{raw_path}/produits",
        'stocks': f"{raw_bucket}/{raw_path}/stocks",
        'ventes': f"{raw_bucket}/{raw_path}/ventes"
    }

    for table_name, source_path in table_paths.items():
        logging.info(f"Leyendo archivos para la tabla {table_name} desde {source_path}")
        
        # Leer todos los archivos Parquet en el directorio de la tabla
        df = spark.read.parquet(f"gs://{source_path}/*")
        
        logging.info(f"Transformando datos para la tabla {table_name}")
        df_transformed = transform_table(df)
        
        logging.info(f"Escribiendo datos transformados para la tabla {table_name} en la zona de procesamiento")
        write_to_processing(df_transformed, table_name, processing_bucket, processing_path)

    logging.info("Transformación y concatenación de tablas completada.")

# Uso
raw_bucket = "kranio-data-lab-ingestion"
processing_bucket = "kranio-data-lab-processing"
raw_path = "raw-zone"
processing_path = ""

process_and_concat_tables(raw_bucket, processing_bucket, raw_path, processing_path)






