CREATE OR REFRESH STREAMING TABLE in_network_file_index (
  in_network_file_index_id STRING,
  index_file_source_id STRING,
  reporting_entity_name STRING,
  reporting_entity_type STRING,
  file_type STRING,
  file_description STRING,
  file_location STRING,
  file_downloaded MAP<STRING, STRING> DEFAULT NULL COMMENT 'Status of the file download, with an error message on failure.',
  CONSTRAINT valid_file_type EXPECT (file_type = 'in_network_files'),
  CONSTRAINT file_location_not_null EXPECT (file_location IS NOT NULL)
)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true'
) AS 
SELECT * FROM (
  FROM STREAM(index_variant_explode_reporting_structure)
  ,LATERAL variant_explode(value) as file_desc
  ,LATERAL variant_explode(file_desc.value) as file_desc_value |>
  WHERE file_type LIKE 'in_network%' |>
  SELECT index_file_source_id, reporting_entity_name, reporting_entity_type, file_type, file_desc.key, file_desc.pos, file_desc_value.key as file_desc_value_key, file_desc_value.pos as file_desc_value_pos, file_desc_value.value as file_desc_value_value|>
  SELECT * EXCEPT (key, file_desc_value_pos) |>
  PIVOT (first(file_desc_value_value) FOR file_desc_value_key IN ('description','plan_id_type','plan_id','plan_market_type','plan_name','location')) |> 
  SELECT index_file_source_id, reporting_entity_name, reporting_entity_type, file_type, description::string as file_description, plan_id_type::string as reporting_plan_id_type, plan_id::string as reporting_plan_id, plan_market_type::string as reporting_plan_market_type, plan_name::string as reporting_plan_name, location::string as file_location |>
  SELECT *, sha2(concat(index_file_source_id, reporting_entity_name, reporting_entity_type, file_type, file_description, file_location), 256) as in_network_file_index_id |>
  SELECT 
    in_network_file_index_id,
    index_file_source_id,
    reporting_entity_name,
    reporting_entity_type,
    file_type,
    file_description,
    file_location
);

CREATE OR REFRESH STREAMING TABLE reporting_plans (
  reporting_plan_index_id STRING,
  index_file_source_id STRING,
  reporting_entity_name STRING,
  reporting_entity_type STRING,
  file_type STRING,
  reporting_plan_id_type STRING,
  reporting_plan_id STRING,
  reporting_plan_market_type STRING,
  reporting_plan_name STRING,
  file_description STRING,
  file_location STRING,
  file_downloaded MAP<STRING, STRING> DEFAULT NULL COMMENT 'Status of the file download, with an error message on failure.',
  CONSTRAINT valid_file_type EXPECT (file_type = 'reporting_plans'),
  CONSTRAINT file_location_is_null EXPECT (file_location IS NULL)
)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true'
) AS 
SELECT * FROM (
  FROM STREAM(index_variant_explode_reporting_structure)
  ,LATERAL variant_explode(value) as file_desc
  ,LATERAL variant_explode(file_desc.value) as file_desc_value |>
  WHERE file_type LIKE 'reporting%' |>
  SELECT index_file_source_id, reporting_entity_name, reporting_entity_type, file_type, file_desc.key, file_desc.pos, file_desc_value.key as file_desc_value_key, file_desc_value.pos as file_desc_value_pos, file_desc_value.value as file_desc_value_value|>
  SELECT * EXCEPT (key, file_desc_value_pos) |>
  PIVOT (first(file_desc_value_value) FOR file_desc_value_key IN ('description','plan_id_type','plan_id','plan_market_type','plan_name','location')) |> 
  SELECT index_file_source_id, reporting_entity_name, reporting_entity_type, file_type, description::string as file_description, plan_id_type::string as reporting_plan_id_type, plan_id::string as reporting_plan_id, plan_market_type::string as reporting_plan_market_type, plan_name::string as reporting_plan_name, location::string as file_location |>
  SELECT *, sha2(concat(index_file_source_id, reporting_entity_name, reporting_entity_type, file_type, reporting_plan_id_type, reporting_plan_id, reporting_plan_market_type, reporting_plan_name, file_description, file_location), 256) as reporting_plan_index_id |>
  SELECT
    reporting_plan_index_id,
    index_file_source_id,
    reporting_entity_name,
    reporting_entity_type,
    file_type,
    reporting_plan_id_type,
    reporting_plan_id,
    reporting_plan_market_type,
    reporting_plan_name,
    file_description,
    file_location
);
