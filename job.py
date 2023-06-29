from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark_iomete.utils import get_spark_logger

JOB_NAME = "sample_job"

"""
This is a sample job that:
- reads a CSV file
- cleans up the column names
- converts the latitude and longitude columns to decimal degrees
- writes the data to an Iceberg table
- reads the data back from the Iceberg table and prints it

This sample is only for demonstration purposes. Feel free to replace the code with your own.
"""
spark = SparkSession.builder.appName(JOB_NAME).getOrCreate()
logger = get_spark_logger(spark=spark)


def transform_cities_csv(input_path):
    logger.info(f"Read csv file(s) from: {input_path}")
    # Read the CSV file and apply the schema
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(input_path)

    print("=====================================")
    print("Shape of the raw cities csv file:")
    df.printSchema()
    df.show()

    # Clean up the column names
    df = df.select([col(c).alias(c.replace(" ", "").replace("\"", "")) for c in df.columns])

    print("=====================================")
    print("Column names after cleaning:")
    df.printSchema()

    # Convert the latitude and longitude columns to decimal degrees
    df = df.withColumn("Latitude", col("LatD") + col("LatM") / 60 + col("LatS") / 3600)
    df = df.withColumn("Longitude", -(col("LonD") + col("LonM") / 60 + col("LonS") / 3600))
    df = df.drop("LatD", "LatM", "LatS", "LonD", "LonM", "LonS")

    # Show the final dataframe
    print("=====================================")
    print("Shape of the cleaned cities csv file:")

    df.printSchema()
    df.show()

    return df


def process(csv_input_path, table_name):
    logger.info(f"Process input: {csv_input_path} and save into table: {table_name}")
    df = transform_cities_csv(input_path=csv_input_path)

    # Write the dataframe to an Iceberg Table (in local lakehouse)
    df.writeTo(table_name).createOrReplace()
    # Note: You can see the iceberg table metadata and data files in the `.lakehouse` directory in the current working
    # directory


if __name__ == '__main__':
    process(
        csv_input_path="gs://iomete-examples/sample-data/csv/cities.csv",
        table_name="cities")
