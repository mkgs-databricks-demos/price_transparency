import dlt
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType, IntegerType, FloatType

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
    def __init__(self, spark: SparkSession, catalog: str, schema: str, volume: str, volume_sub_path: str, file_type: str, file_desc: str, maxFilesPerTrigger: int, cleanSource_retentionDuration: str, cleanSource: str = "OFF", cloudFiles_useNotifications: str = "false"):
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
        
        # Assign schema for in-network file type
        if self.file_type == "in-network":
            self.jsonSchema = StructType([
                StructField("reporting_entity_name", StringType(), True),
                StructField("reporting_entity_type", StringType(), True),
                StructField("plan_name", StringType(), True),
                StructField("plan_id_type", StringType(), True),
                StructField("plan_id", StringType(), True),
                StructField("plan_market_type", StringType(), True),
                StructField("last_updated_on", StringType(), True),
                StructField("version", StringType(), True),
                StructField("provider_references", ArrayType(
                    StructType([
                        StructField("provider_group_id", IntegerType(), True),
                        StructField("provider_groups", ArrayType(
                            StructType([
                                StructField("npi", ArrayType(IntegerType()), True),
                                StructField("tin", StructType([
                                    StructField("type", StringType(), True),
                                    StructField("value", StringType(), True)
                                ]), True)
                            ])
                        ), True),
                        StructField("location", StringType(), True)
                    ])
                ), True),
                StructField("in_network", ArrayType(
                    StructType([
                        StructField("negotiation_arrangement", StringType(), True),
                        StructField("name", StringType(), True),
                        StructField("billing_code_type", StringType(), True),
                        StructField("billing_code_type_version", StringType(), True),
                        StructField("billing_code", StringType(), True),
                        StructField("description", StringType(), True),
                        StructField("negotiated_rates", ArrayType(
                            StructType([
                                StructField("negotiated_prices", ArrayType(
                                    StructType([
                                        StructField("service_code", ArrayType(StringType()), True),
                                        StructField("billing_class", StringType(), True),
                                        StructField("negotiated_type", StringType(), True),
                                        StructField("billing_code_modifier", ArrayType(StringType()), True),
                                        StructField("negotiated_rate", DoubleType(), True),
                                        StructField("expiration_date", StringType(), True),
                                        StructField("additional_information", StringType(), True)
                                    ])
                                ), True),
                                StructField("provider_groups", ArrayType(
                                    StructType([
                                        StructField("npi", ArrayType(IntegerType()), True),
                                        StructField("tin", StructType([
                                            StructField("type", StringType(), True),
                                            StructField("value", StringType(), True)
                                        ]), True)
                                    ])
                                ), True),
                                StructField("provider_references", ArrayType(IntegerType()), True)
                            ])
                        ), True),
                        StructField("covered_services", ArrayType(
                            StructType([
                                StructField("billing_code_type", StringType(), True),
                                StructField("billing_code_type_version", StringType(), True),
                                StructField("billing_code", StringType(), True),
                                StructField("description", StringType(), True)
                            ])
                        ), True),
                        StructField("bundled_codes", ArrayType(
                            StructType([
                                StructField("billing_code_type", StringType(), True),
                                StructField("billing_code_type_version", StringType(), True),
                                StructField("billing_code", StringType(), True),
                                StructField("description", StringType(), True)
                            ])
                        ), True)
                    ])
                ), True)
            ])
        else:
            self.jsonSchema = None

    def mrf_ingest(self):
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
                'delta.autoOptimize.autoCompact': 'true'
            },
            cluster_by_auto=True
        )
        @dlt.expect_or_drop("valid_json", "_rescued_data is null")
        def mrf_ingest_def():
            reader = (
                self.spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.schemaLocation", f"/tmp/schema/{self.file_type}_bronze")  # Persist schema for evolution
                .option("cloudFiles.inferColumnTypes", "false")  # Use explicit schema
                .option("cloudFiles.useNotifications", self.cloudFiles_useNotifications)
                .option("cloudFiles.cleanSource", self.cleanSource)
                .option("cloudFiles.cleanSource.retentionDuration", self.cleanSource_retentionDuration)
                .option("maxFilesPerTrigger", self.maxFilesPerTrigger)
                .option("fullText", "true")
            )
            if self.jsonSchema:
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