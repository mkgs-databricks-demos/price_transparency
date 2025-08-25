# The 'data_sources' folder contains definitions for all data sources
# Keeping 'data_sources' separate provides a clear overview of the data used
# and allows for easy swapping of sources during development.

from utilities.bronze import Bronze
import os
import json
config_dir = "../config/file_ingestion"
json_files = [f for f in os.listdir(config_dir) if f.endswith('.json')]

definitions = []
for json_file in json_files:
    with open(os.path.join(config_dir, json_file), 'r') as file:
        definitions.append(json.load(file))

for definition in definitions:
    BronzePipeline = Bronze(
        spark = spark
        ,catalog = spark.conf.get("catalog")
        ,schema = spark.conf.get("schema")
        ,volume = spark.conf.get("volume")
        ,volume_sub_path = definition["volume_sub_path"]
        ,file_type = definition["file_type"]
        ,file_desc = definition["file_desc"]
        ,cleanSource = definition["cleanSource"]
        ,cleanSource_retentionDuration = definition["cleanSource_retentionDuration"]
    )
    
    BronzePipeline.stream_ingest()