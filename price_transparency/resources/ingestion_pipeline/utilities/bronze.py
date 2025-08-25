import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, lit, concat
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, MapType

class Bronze:
    def __init__(self, spark: SparkSession, catalog: str, schema: str, volume: str, volume_sub_path: str, file_type: str, file_desc: str, cleanSource_retentionDuration: str, cleanSource: str = "OFF"):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self.volume_sub_path = volume_sub_path
        self.file_type = file_type
        self.file_desc = file_desc
        self.cleanSource_retentionDuration = cleanSource_retentionDuration
        self.cleanSource = cleanSource
    """
    The Bronze class represents a data structure for managing metadata related to a specific data resource.
    
    Attributes:
        spark (SparkSession): The SparkSession object used for interacting with the Spark runtime.
        catalog (str): The catalog name where the data is stored.
        schema (str): The schema name within the catalog.
        volume (str): The volume name where the data is stored.
        volume_sub_path (str): The sub-path within the volume where the data is located.
        file_type (str): The type of the data resource.
        file_desc (str): Description of the file type.
        cleanSource_retentionDuration (str): Retention duration for cleaning the source.
        cleanSource (str): Clean source option, default is "OFF".

    Methods:
        __repr__(): Returns a string representation of the Bronze object.
        stream_ingest(): Defines a Delta Live Table for streaming ingestion of files.
        to_dict(): Converts the Bronze object attributes to a dictionary.
        from_dict(cls, data): Creates a Bronze object from a dictionary.
    """

    def __repr__(self):
        return (f"Bronze(spark={self.spark!r}, catalog={self.catalog!r}, schema={self.schema!r}, "
                f"volume={self.volume!r}, volume_sub_path={self.volume_sub_path!r}, "
                f"file_type={self.file_type!r}, file_desc={self.file_desc!r}, "
                f"cleanSource_retentionDuration={self.cleanSource_retentionDuration!r}, "
                f"cleanSource={self.cleanSource!r})")

    @staticmethod
    @udf(MapType(StringType(), StringType()))
    def copy_file(src_path, dest_path):
      try:
        with open(src_path, 'rb') as src_file, open(dest_path, 'wb') as dest_file:
          dest_file.write(src_file.read())
        return {"status": "success"}
      except Exception as e:
        return {"status": "error", "message": str(e)}
      
    def stream_ingest(self):
      schema_definition = f"""
        file_metadata STRUCT < file_path: STRING, 
        file_name: STRING,
        file_size: BIGINT,
        file_block_start: BIGINT,
        file_block_length: BIGINT,
        file_modification_time: TIMESTAMP > NOT NULL COMMENT 'Metadata about the file ingested.'
        ,ingest_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'The date timestamp the file was ingested.'
        ,value STRING COMMENT 'The raw {self.file_desc} file contents.'
      """

      volume_path = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"
      if self.volume_sub_path:
        volume_path = f"{volume_path}/{self.volume_sub_path}"

      @dlt.table(
        name=f"{self.catalog}.{self.schema}.{self.file_type}_bronze",
        comment=f"Streaming bronze ingestion of {self.file_type} files from {volume_path}",
        # spark_conf={"<key>" : "<value>", "<key>" : "<value>"},
        table_properties={
          'quality' : 'bronze'
          ,'delta.enableChangeDataFeed' : 'true'
          ,'delta.enableDeletionVectors' : 'true'
          ,'delta.enableRowTracking' : 'true'
        },
        # path="<storage-location-path>",
        # partition_cols=["<partition-column>", "<partition-column>"],
        cluster_by_auto=True,
        cluster_by = ["file_metadata.file_path"],
        schema=schema_definition,
        # row_filter = "row-filter-clause",
        temporary=False
      )
      # @dlt.expect(...)
      def stream_ingest_function():
          return (self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "text")
            .option("wholeText", "true")
            .option("cloudFiles.cleanSource", self.cleanSource)
            .option("cloudFiles.cleanSource.retentionDuration", self.cleanSource_retentionDuration)
            .load(volume_path)
            .selectExpr("_metadata as file_metadata", "*")
          )

    def to_dict(self):
        return {
            "spark": self.spark,
            "catalog": self.catalog,
            "schema": self.schema,
            "volume": self.volume,
            "volume_sub_path": self.volume_sub_path,
            "file_type": self.file_type,
            "file_desc": self.file_desc,
            "cleanSource_retentionDuration": self.cleanSource_retentionDuration,
            "cleanSource": self.cleanSource
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            spark=data['spark'],
            catalog=data['catalog'],
            schema=data['schema'],
            volume=data['volume'],
            volume_sub_path=data['volume_sub_path'],
            file_type=data['file_type'],
            file_desc=data['file_desc'],
            cleanSource_retentionDuration=data['cleanSource_retentionDuration'],
            cleanSource=data.get('cleanSource', "OFF")
        )









