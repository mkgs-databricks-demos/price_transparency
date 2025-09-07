from utilities.utils import MachineReadableFiles

definition = {
  "file_type": "in_network_json",
  "file_desc": "in network price transparency json",
  "file_extention": "json",
  "cleanSource": "OFF",
  "cleanSource_retentionDuration": "7 days",
  "volume_sub_path": "in-network/uncompressed",
  # "maxFilesPerTrigger": 100,
  "cloudFiles_useNotifications": "false"
}

mrf = MachineReadableFiles(
    spark = spark
    ,catalog = "mgiglia"
    ,schema = "dev_matthew_giglia_price_transparency"
    ,volume= "landing"
    ,volume_sub_path=definition.get("volume_sub_path", "in-network/uncompressed")
    ,file_type=definition.get("file_type", "in_network_json")
    ,file_desc=definition.get("file_desc")
    ,cleanSource=definition.get("cleanSource", "OFF")
    ,cleanSource_retentionDuration=definition.get("cleanSource_retentionDuration", "7 days")
    ,maxFilesPerTrigger=definition.get("maxFilesPerTrigger", 100)
    ,cloudFiles_useNotifications=definition.get("cloudFiles_useNotifications", "true")
)

mrf.ingest()