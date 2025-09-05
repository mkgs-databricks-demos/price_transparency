import dlt
from utilities import *
from pyspark.sql.functions import col, expr

@dlt.view()
def allowed_amount_file_index_cdf():
  return (
    spark.readStream.option("readChangeFeed", "true")
    .table("allowed_amount_file_index")
    .filter(col("file_downloaded").isNull())
    .filter(col("file_location").isNotNull())
    .withColumn("file_downloaded", lit('{"status": "success"}'))
  )

dlt.create_streaming_table("allowed_amount_file_index")

dlt.create_auto_cdc_flow(
    target="allowed_amount_file_index",
    source="allowed_amount_file_index_cdf",
    keys=["allowed_amount_file_index_id"],
    sequence_by=col("_commit_timestamp"),
    apply_as_deletes=expr("_change_type = 'delete'"),
    apply_as_truncates=expr("_change_type = 'truncate'"),
    except_column_list=["_change_type", "_commmit_version", "_commit_timestamp"],
    stored_as_scd_type=1  
)

@dlt.table(
)
def undownload_files():
  return (
    spark.readStream.table("allowed_amount_file_index_cdf")
    .filter(col("file_downloaded").isNull())
    .filter(col("file_location").isNotNull())
    .selectExpr("*", "_change_type as change_type", "_commit_timestamp as commit_timestamp", "_commit_version as commit_version")
  )