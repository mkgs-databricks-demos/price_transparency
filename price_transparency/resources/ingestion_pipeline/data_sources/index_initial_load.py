import dlt
import os
import sys

sys_path_capture = [p for p in sys.path if p.endswith('price_transparency/price_transparency/resources/ingestion_pipeline/data_sources')]
fixtures_dir = sys_path_capture[0] + "/.../../../../../fixtures"
print(fixtures_dir)

@dlt.table(
)
def index_initial_bronze():
  return (
    spark.read.format("text")
    .load("/Workspace/Users/matthew.giglia@databricks.com/price_transparency/price_transparency/fixtures")
    .selectExpr("*", "CURRENT_TIMESTAMP() as ingest_time")
  )

  price_transparency/fixtures
  