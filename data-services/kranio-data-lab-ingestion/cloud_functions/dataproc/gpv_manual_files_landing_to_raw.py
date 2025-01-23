import argparse
import logging

import pyspark.sql.functions as F

import pyspark.sql.types as T
from pyspark.sql import SparkSession
from stringcase import snakecase
from unidecode import unidecode

spark_session = SparkSession.builder.appName('ingestion-raw').getOrCreate()

logging.basicConfig(
    level=logging.INFO, format='%(asctime)-23s   %(levelname)-9s:   %(message)s'
)


class InputStandardizer:
    def __init__(self, spark_session, dataframe, options_dict):
        self.spark_sesion = spark_session
        self.input_df = dataframe
        self.options_dict = options_dict

        self.custom_replaces = self.options_dict.get('custom_replaces', {})
        self.custom_renames = self.options_dict.get('custom_renames', {})

        self.output_df = None

    @staticmethod
    def apply_custom_replaces(column_name, custom_replaces):
        for old_chunck, new_chunk in custom_replaces.items():
            column_name = column_name.replace(old_chunck, new_chunk)

        return column_name

    @staticmethod
    def convert_to_snake_case(column_name):
        return snakecase(column_name)

    @staticmethod
    def remove_unwanted_characters(column_name):
        new_name = ''
        for char in column_name:
            if char.isalnum() or (char in '_'):
                new_name += char

        return new_name

    @staticmethod
    def remove_double_underscore(column_name):
        while '__' in column_name:
            column_name = column_name.replace('__', '_')

        return column_name

    @staticmethod
    def strip_underscores(column_name):
        return column_name.strip('_')

    @staticmethod
    def remove_accent_marks(column_name):
        return unidecode(column_name)

    @classmethod
    def standardize_column(cls, column_name, custom_replaces={}):
        new_column_name = column_name
        if custom_replaces:
            new_column_name = cls.apply_custom_replaces(column_name, custom_replaces)

        new_column_name = cls.convert_to_snake_case(new_column_name)
        new_column_name = cls.remove_unwanted_characters(new_column_name)
        new_column_name = cls.remove_double_underscore(new_column_name)
        new_column_name = cls.strip_underscores(new_column_name)
        new_column_name = cls.remove_accent_marks(new_column_name)

        return new_column_name

    def standardize(self):
        for old_name, new_name in self.custom_renames.items():
            self.input_df = self.input_df.withColumnRenamed(old_name, new_name)

        # columns = list(set(self.old_columns_names) - set(self.custom_renames.values()))

        new_columns = [
            F.col(original_name).alias(
                self.standardize_column(original_name, self.custom_replaces)
            )
            for original_name in self.input_df.columns
        ]

        self.output_df = self.input_df.select(
            *new_columns
        )  # , *self.custom_renames.values())

        return self


options_dict = {
    'custom_replaces': {
        '<': 'menor',
        '%': '_porciento',
        'N°': 'num',
    },
    'custom_renames': {
        'Customer ID': 'customer_id',
        'Product Name': 'product_name',
    },
}


def handle_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_path', default='input', help='Input path in GCS')
    parser.add_argument('--output_path', default='output', help='Output path in GCS')
    parser.add_argument('--stage', default='dev', help='Stage name')
    parser.add_argument(
        '--ingestion_date',
        required=True,
        help='Date from ETL orchestration tool',
    )
    parser.add_argument(
        '--execution_date',
        required=True,
        help='Date from ETL orchestration tool',
    )

    args = parser.parse_args()

    return args


def read_and_create_schema(path, sep, encoding):
    df = spark_session.read.csv(path, sep=sep, header=True)
    colnames = df.columns
    schema = T.StructType(
        [T.StructField(name, T.StringType(), True) for name in colnames]
    )
    df = spark_session.read.load(
        path=path,
        encoding=encoding,
        format='csv',
        header=True,
        sep=sep,
        schema=schema,
    )
    return df


def create_ingestion_date_column(df, ingestion_date):
    return df.withColumn('ingestion_date', F.lit(ingestion_date))


def create_execution_date_column(df, execution_date):
    return df.withColumn('execution_datetime', F.lit(execution_date))


def add_partition_columns(df, ingestion_date):
    ingestion_date = ingestion_date.replace('-', '')
    ingestion_year = ingestion_date[0:4]
    ingestion_month = ingestion_date[4:6]
    ingestion_day = ingestion_date[6:8]
    return (
        df.withColumn('ingestion_year', F.lit(ingestion_year))
        .withColumn('ingestion_month', F.lit(ingestion_month))
        .withColumn('ingestion_day', F.lit(ingestion_day))
    )


def standardize_columns(df):
    standardizer = InputStandardizer(spark_session, df, options_dict)
    new_df = standardizer.standardize().output_df
    return new_df


def save_to_gcs(df, path):
    (
        df.coalesce(1)
        .write.option('encoding', 'UTF-8')
        .option('sep', ';')
        .option('header', 'true')
        .partitionBy('ingestion_year', 'ingestion_month', 'ingestion_day')
        .mode('overwrite')
        .option('partitionOverwriteMode', 'dynamic')
        .format('parquet')
        .save(path)
    )


def run_step(input_path, output_path, sep, encoding, ingestion_date, execution_date):
    csv_path = input_path
    output_path = output_path
    df = read_and_create_schema(csv_path, sep, encoding=encoding)
    df = standardize_columns(df)
    df = create_ingestion_date_column(df, ingestion_date)
    df = create_execution_date_column(df, execution_date)
    df = add_partition_columns(df, ingestion_date)
    save_to_gcs(df, output_path)

    return df


args = handle_arguments()
stage = args.stage
ingestion_date = args.ingestion_date
execution_date = args.execution_date


ingestion_bucket = f'kranio-data-lab-ingestion'
csv_input_prefix = f'gs://{ingestion_bucket}/landing-zone'
output_prefix = f'gs://{ingestion_bucket}/raw-zone'

ingestion_metadata = {
    'clients': {
        'input_file': 'clients.csv',
        'output_path': 'clients',
        'sep': ';',
        'encoding': 'UTF-8',
    },
    'produits': {
        'input_file': 'produits.csv',
        'output_path': 'produits',
        'sep': ';',
        'encoding': 'UTF-8',
    },
    'stocks': {
        'input_file': 'stocks.csv',
        'output_path': 'stocks',
        'sep': ';',
        'encoding': 'UTF-8',
    },
    'ventes': {
        'input_file': 'ventes.csv',
        'output_path': 'ventes',
        'sep': ';',
        'encoding': 'UTF-8',
    },
}

outputs_dict = {}
for table_name, params in ingestion_metadata.items():
    logging.info(f'Processing {table_name}...')

    sep = params['sep']
    encoding = params['encoding']
    input_file = params['input_file']
    output_path = params['output_path']
    full_input_path = f'{csv_input_prefix}/{input_file}'
    full_output_path = f'{output_prefix}/{output_path}'

    df = run_step(
        full_input_path, full_output_path, sep, encoding, ingestion_date, execution_date
    )
    outputs_dict[table_name] = df
