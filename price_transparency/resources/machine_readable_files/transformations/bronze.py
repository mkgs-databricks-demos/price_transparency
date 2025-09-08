from utilities.utils import MachineReadableFiles
import os
import json
config_dir = "../config"
definition_files = [f for f in os.listdir(config_dir) if f.endswith('def.json')]

definitions = []
for definition_file in definition_files:
    with open(os.path.join(config_dir, definition_file), 'r') as file:
        definitions.append(json.load(file))

for definition in definitions:
  mrf = MachineReadableFiles(
      spark = spark
      ,catalog = "mgiglia"
      ,schema = "dev_matthew_giglia_price_transparency"
      ,volume= "landing"
      ,volume_sub_path=definition.get("volume_sub_path")
      ,file_type=definition.get("file_type")
      ,file_desc=definition.get("file_desc")
      ,useSchemaHints = definition.get("useSchemaHints")
      ,schemaHints = definition.get("schemaHints")
      ,jsonSchema = definition.get("jsonSchema")
      ,cleanSource=definition.get("cleanSource", "OFF")
      ,cleanSource_retentionDuration=definition.get("cleanSource_retentionDuration", "7 days")
      ,maxFilesPerTrigger=definition.get("maxFilesPerTrigger", 100)
      ,cloudFiles_useNotifications=definition.get("cloudFiles_useNotifications", "true")
  )

  mrf.ingest()