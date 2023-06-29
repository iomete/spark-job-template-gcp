from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark_iomete.test_utils import table_name_with_random_suffix

import job

LOCAL_INPUT_PATH = "test_data/cities.csv"

spark = SparkSession.builder.appName(job.JOB_NAME + "-test").getOrCreate()


def test_transform_cities_csv():
    cities_df = job.transform_cities_csv(input_path=LOCAL_INPUT_PATH)

    """
    root
     |-- NS: string (nullable = true)
     |-- EW: string (nullable = true)
     |-- City: string (nullable = true)
     |-- State: string (nullable = true)
     |-- Latitude: double (nullable = true)
     |-- Longitude: double (nullable = true)
    """
    expected_schema = StructType([
        StructField("NS", StringType(), True),
        StructField("EW", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Latitude", DoubleType(), True),
        StructField("Longitude", DoubleType(), True),
    ])

    assert cities_df.schema == expected_schema
    assert cities_df.count() == 128


def test_process():
    # Setup: create a random table name
    table_name = table_name_with_random_suffix("vestner_events")

    # Act: Cleanup cities csv file and write to Iceberg table
    job.process(csv_input_path=LOCAL_INPUT_PATH, table_name=table_name)

    # Assert: Read the data back from the Iceberg table and confirm it is correct
    df = spark.table(table_name)
    assert df.count() == 128
    assert df.schema == job.transform_cities_csv(input_path=LOCAL_INPUT_PATH).schema
