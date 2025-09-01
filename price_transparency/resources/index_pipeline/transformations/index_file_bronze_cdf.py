import dlt

@dlt.view()
def index_file_cdf():
    df = spark.readStream.option("readChangeFeed", "true").table("index_json_bronze")
    return df

@dlt.table(
  temporary=True
  ,table_properties={
    'delta.feature.variantType-preview': 'supported'
  }
)
def index_variant_bronze():
    df = spark.readStream.table("index_file_cdf")
    return df.selectExpr(
        "index_file_source_id",
        "file_metadata",
        "ingest_time",
        "try_parse_json(value) as variant_col"
    )

@dlt.table(
  temporary=True
  ,table_properties={
    'delta.feature.variantType-preview': 'supported'
  }
)
def index_variant_explode():
    df = spark.sql("""
        FROM STREAM(index_variant_bronze)
        ,LATERAL variant_explode(variant_col) |>
        SELECT index_file_source_id, file_metadata, ingest_time, key, value |>
        PIVOT (first(value) FOR key IN ('reporting_entity_name', 'reporting_entity_type', 'reporting_structure')) |>
        SELECT index_file_source_id, file_metadata, ingest_time, reporting_entity_name::string, reporting_entity_type::string, reporting_structure         
    """)
    return df
  
@dlt.table(
  temporary=True
  ,table_properties={
    'delta.feature.variantType-preview': 'supported'
  }
)
def index_variant_explode_reporting_structure():
    df = spark.sql("""
        FROM STREAM(index_variant_explode)
        ,LATERAL variant_explode(reporting_structure) as reporting_structure
        ,LATERAL variant_explode(reporting_structure.value) as reporting_structure_value |>
        SELECT index_file_source_id, file_metadata, ingest_time, reporting_entity_name, reporting_entity_type, reporting_structure_value.key as file_type, reporting_structure_value.value
    """)
    return df

