import dlt
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql.types import * # StructType, StructField, StringType, ArrayType, DoubleType, IntegerType, FloatType, VariantType

@udf(returnType=FloatType())
def distance_km(distance_miles):
    """Convert distance from miles to kilometers (1 mile = 1.60934 km)."""
    return distance_miles * 1.60934

class MachineReadableFiles:
    """
    Class for ingesting machine readable files using Delta Live Tables and cloudFiles (Auto Loader).
    Attributes:
        spark: SparkSession object
        catalog, schema, volume, volume_sub_path: Used to construct the source path for ingestion
        file_type, file_desc: Used for table naming and comments
        maxFilesPerTrigger: Controls batch size for streaming ingestion
        cleanSource_retentionDuration, cleanSource: Controls source file cleanup
        cloudFiles_useNotifications: Enables event-based notifications for new files
        jsonSchema: Explicit schema for JSON files (set if file_type == "in-network")
    """
    def __init__(self, spark: SparkSession, catalog: str, schema: str, volume: str,  file_type: str, file_desc: str, maxFilesPerTrigger: int, cleanSource_retentionDuration: str, volume_sub_path: str = None, cleanSource: str = "OFF", cloudFiles_useNotifications: str = "false", schemaHints: str = None, jsonSchema: str = None, useSchemaHints: bool = True):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self.volume_sub_path = volume_sub_path
        self.file_type = file_type
        self.file_desc = file_desc
        self.maxFilesPerTrigger = maxFilesPerTrigger
        self.cleanSource_retentionDuration = cleanSource_retentionDuration
        self.cleanSource = cleanSource
        self.cloudFiles_useNotifications = cloudFiles_useNotifications
        self.schemaHints = schemaHints
        self.jsonSchema = jsonSchema
        self.useSchemaHints = useSchemaHints
        
        
    def ingest(self):
        """
        Indexes files in the volume and copies them to the destination path using best practices for cloudFiles with JSON.
        """
        volume_path = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"
        if self.volume_sub_path:
            volume_path = f"{volume_path}/{self.volume_sub_path}"

        @dlt.table(
            name=f"{self.catalog}.{self.schema}.{self.file_type}_bronze",
            comment=f"Streaming bronze ingestion of {self.file_type} machine readable files from {volume_path}",
            table_properties={
                'quality': 'bronze',
                'delta.enableChangeDataFeed': 'true',
                'delta.enableDeletionVectors': 'true',
                'delta.enableRowTracking': 'true',
                'delta.autoOptimize.optimizeWrite': 'true',
                'delta.autoOptimize.autoCompact': 'true',
                'delta.feature.variantType-preview': 'supported'
            },
            cluster_by_auto=True
        )
        @dlt.expect_or_drop("valid_json", "_rescued_data is null")
        def mrf_ingest_def():
            reader = (
                self.spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.inferColumnTypes", "false")  # Use explicit schema
                .option("cloudFiles.useNotifications", self.cloudFiles_useNotifications)
                .option("cloudFiles.cleanSource", self.cleanSource)
                .option("cloudFiles.cleanSource.retentionDuration", self.cleanSource_retentionDuration)
                .option("maxFilesPerTrigger", self.maxFilesPerTrigger)
                .option("rescuedDataColumn", "_rescued_data")
                .option("cloudFiles.schemaEvolutionMode", "rescue")
                .option("multiLine", "true")
            )
            if self.useSchemaHints:
                df = reader.option("cloudFiles.schemaHints", self.schemaHints).load(volume_path)
            elif self.jsonSchema:
                df = reader.schema(self.jsonSchema).load(volume_path)
            else:
                df = reader.load(volume_path)
            return (
                df.selectExpr(
                    "sha2(concat(_metadata.*), 256) as index_file_source_id",
                    "_metadata as file_metadata",
                    "CURRENT_TIMESTAMP() as ingest_time",
                    "_metadata.file_modification_time as rcrd_timestamp",
                    "*"
                )
            )