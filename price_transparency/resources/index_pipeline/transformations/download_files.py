import dlt
from utilities import *
from pyspark.sql.functions import col

@dlt.view()
def allowed_amount_file_index_cdf():
  return spark.readStream.option("readChangeFeed", "true").table("allowed_amount_file_index")

@dlt.table(
)
def download_files():
  df = (
    spark.read.table("allowed_amount_file_index_cdf")
    .filter(col("file_downloaded").isNull())
    .filter(col("file_location").isNotNull())
  )