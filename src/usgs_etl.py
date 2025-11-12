from pyspark.sql import SparkSession
from datetime import date, timedelta
import reverse_geocoder as rg
import os

# Suppress Hadoop warnings for Windows
os.environ['HADOOP_HOME'] = os.path.dirname(os.path.abspath(__file__))

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EarthquakeDataProcessing") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "./spark-warehouse") \
    .config("spark.driver.host", "localhost") \
    .enableHiveSupport() \
    .getOrCreate()

# Set log level to reduce console output
spark.sparkContext.setLogLevel("ERROR")

start_date = date.today() - timedelta(7)
input_dir = f'./data/raw'

# Load the JSON data into a Spark DataFrame
df = spark.read.option("multiline", "true").json(f"{input_dir}/{start_date}_earthquake_data.json")

# Register the DataFrame as a temporary view
df.createOrReplaceTempView("raw_earthquake_data")

# Reshape, validate, and transform earthquake data using SQL
final_df = spark.sql("""
    SELECT 
        id,
        COALESCE(geometry.coordinates[0], 0) as longitude,
        COALESCE(geometry.coordinates[1], 0) as latitude,
        geometry.coordinates[2] as elevation,
        properties.title as title,
        properties.place as place_description,
        properties.sig as sig,
        (case
            when properties.sig < 100 then 'Low'
            when properties.sig >= 100 and properties.sig < 500 then 'Moderate'
            when properties.sig > 500 then 'High'
        end) as sig_class, 
        properties.mag as mag,
        properties.magType as magType,
        CAST(COALESCE(properties.time, 0) / 1000 AS TIMESTAMP) as time,
        CAST(COALESCE(properties.updated, 0) / 1000 AS TIMESTAMP) as updated
    FROM raw_earthquake_data
""")

# Show some results to verify it worked
print(f"Processing {final_df.count()} earthquake records")
final_df.show(5)

# Stop the Spark session
spark.stop()