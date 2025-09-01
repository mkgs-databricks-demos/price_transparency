import dlt
from utilities import *

@dlt.table(
)
def download_files():
  df = spark.read.table("allowed_amount_file_index")