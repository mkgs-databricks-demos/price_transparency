CREATE OR REFRESH STREAMING TABLE allowed_amount_file_index (
  allowed_amount_file_index_id STRING,
  index_file_source_id STRING,
  reporting_entity_name STRING,
  reporting_entity_type STRING,
  file_type STRING,
  file_description STRING,
  file_location STRING,
  file_downloaded MAP<STRING, STRING> DEFAULT NULL COMMENT 'Status of the file download, with an error message on failure.',
  rcrd_timestamp TIMESTAMP,
  CONSTRAINT valid_file_type EXPECT (file_type = 'allowed_amount_file'),
  CONSTRAINT file_location_not_null EXPECT (file_location IS NOT NULL)
)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true'
);

CREATE FLOW fl_allowed_amount_file_index AS AUTO CDC INTO allowed_amount_file_index 
FROM (
  FROM (FROM (FROM (FROM STREAM(index_json_bronze_cdf) WATERMARK rcrd_timestamp DELAY OF INTERVAL 1 MILLISECOND |> 
  WHERE rcrd_timestamp IS NOT NULL |>
  SELECT index_file_source_id, file_metadata, WINDOW(rcrd_timestamp, '1 MILLISECOND') as _timestamp_window, try_parse_json(value) as variant_col, _commit_timestamp, _change_type, _commit_version)
  ,LATERAL variant_explode(variant_col) |>
  SELECT index_file_source_id, file_metadata, _timestamp_window, key, value, _commit_timestamp, _change_type, _commit_version |>
  PIVOT (first(value) FOR key IN ('reporting_entity_name', 'reporting_entity_type', 'reporting_structure')) |>
  SELECT index_file_source_id, file_metadata, _timestamp_window, reporting_entity_name::string, reporting_entity_type::string, reporting_structure, _commit_timestamp, _change_type, _commit_version)
  ,LATERAL variant_explode(reporting_structure) as reporting_structure
  ,LATERAL variant_explode(reporting_structure.value) as reporting_structure_value |>
  SELECT index_file_source_id, file_metadata, _timestamp_window, reporting_entity_name, reporting_entity_type, reporting_structure_value.key as file_type, reporting_structure_value.value, _commit_timestamp, _change_type, _commit_version)
  ,LATERAL variant_explode(value) as file_desc
  ,LATERAL variant_explode(file_desc.value) as file_desc_value |>
  -- WHERE file_type LIKE 'allowed%' |>
  SELECT index_file_source_id, file_metadata, _timestamp_window, reporting_entity_name, reporting_entity_type, file_type, file_desc.key, file_desc.pos, file_desc_value.key as file_desc_value_key, file_desc_value.pos as file_desc_value_pos, file_desc_value.value as file_desc_value_value, _commit_timestamp, _change_type, _commit_version |>
  SELECT * EXCEPT (key, file_desc_value_pos) |>
  PIVOT (first(file_desc_value_value) FOR file_desc_value_key IN ('description','plan_id_type','plan_id','plan_market_type','plan_name','location')) |> 
  SELECT index_file_source_id, file_metadata, _timestamp_window, reporting_entity_name, reporting_entity_type, file_type, description::string as file_description, plan_id_type::string as reporting_plan_id_type, plan_id::string as reporting_plan_id, plan_market_type::string as reporting_plan_market_type, plan_name::string as reporting_plan_name, location::string as file_location, _commit_timestamp, _change_type, _commit_version |>
  SELECT *, sha2(concat(index_file_source_id, reporting_entity_name, reporting_entity_type, file_type, file_description), 256) as allowed_amount_file_index_id, current_timestamp() as rcrd_timestamp
)
KEYS
  (index_file_source_id, reporting_entity_name, reporting_entity_type, file_type, file_description)
APPLY AS DELETE WHEN
  _change_type = "delete"
APPLY AS TRUNCATE WHEN
  _change_type = "truncate"
SEQUENCE BY
  (_timestamp_window, _commit_timestamp)
COLUMNS * EXCEPT
  (file_metadata, reporting_plan_id_type, reporting_plan_id, reporting_plan_market_type, reporting_plan_name, _timestamp_window, _change_type, _commit_version, _commit_timestamp)
STORED AS
  SCD TYPE 1;

-- CREATE FLOW fl_allowed_amount_file_index AS AUTO CDC INTO 
-- allowed_amount_file_index FROM (
--   FROM (FROM STREAM(index_variant_explode_reporting_structure_cdf) WATERMARK rcrd_timestamp DELAY OF INTERVAL 1 MILLISECOND |> 
--   WHERE rcrd_timestamp IS NOT NULL |>
--   SELECT *, window(rcrd_timestamp, '1 MILLISECOND') as _commit_window)
--   ,LATERAL variant_explode(value) as file_desc
--   ,LATERAL variant_explode(file_desc.value) as file_desc_value |>
--   -- WHERE file_type LIKE 'allowed%' |>
--   SELECT index_file_source_id, file_metadata, reporting_entity_name, reporting_entity_type, file_type, file_desc.key, file_desc.pos, file_desc_value.key as file_desc_value_key, file_desc_value.pos as file_desc_value_pos, file_desc_value.value as file_desc_value_value, _commit_window, _change_type, _commit_version |>
--   SELECT * EXCEPT (key, file_desc_value_pos) |>
--   PIVOT (first(file_desc_value_value) FOR file_desc_value_key IN ('description','plan_id_type','plan_id','plan_market_type','plan_name','location')) |> 
--   SELECT index_file_source_id, file_metadata, reporting_entity_name, reporting_entity_type, file_type, description::string as file_description, plan_id_type::string as reporting_plan_id_type, plan_id::string as reporting_plan_id, plan_market_type::string as reporting_plan_market_type, plan_name::string as reporting_plan_name, location::string as file_location, _commit_window, _change_type, _commit_version |>
--   SELECT *, sha2(concat(index_file_source_id, reporting_entity_name, reporting_entity_type, file_type, file_description), 256) as allowed_amount_file_index_id, current_timestamp() as rcrd_timestamp
-- )
-- KEYS
--   (index_file_source_id, reporting_entity_name, reporting_entity_type, file_type, file_description)
-- APPLY AS DELETE WHEN
--   _change_type = "delete"
-- APPLY AS TRUNCATE WHEN
--   _change_type = "truncate"
-- SEQUENCE BY
--   (rcrd_timestamp)
-- COLUMNS * EXCEPT
--   (file_metadata, reporting_plan_id_type, reporting_plan_id, reporting_plan_market_type, reporting_plan_name, _change_type, _commit_version, _commit_window)
-- STORED AS
--   SCD TYPE 1;




-- CREATE OR REFRESH STREAMING TABLE allowed_amount_file_index (
--   allowed_amount_file_index_id STRING,
--   index_file_source_id STRING,
--   reporting_entity_name STRING,
--   reporting_entity_type STRING,
--   file_type STRING,
--   file_description STRING,
--   file_location STRING,
--   file_downloaded MAP<STRING, STRING> DEFAULT NULL COMMENT 'Status of the file download, with an error message on failure.',
--   CONSTRAINT valid_file_type EXPECT (file_type = 'allowed_amount_file'),
--   CONSTRAINT file_location_not_null EXPECT (file_location IS NOT NULL)
-- )
-- TBLPROPERTIES (
--   'delta.autoOptimize.optimizeWrite' = 'true',
--   'delta.autoOptimize.autoCompact' = 'true',
--   'delta.enableChangeDataFeed' = 'true',
--   'delta.enableDeletionVectors' = 'true',
--   'delta.enableRowTracking' = 'true'
-- ) AS 
-- SELECT * FROM (
--   FROM STREAM(index_variant_explode_reporting_structure)
--   ,LATERAL variant_explode(value) as file_desc
--   ,LATERAL variant_explode(file_desc.value) as file_desc_value |>
--   WHERE file_type LIKE 'allowed%' |>
--   SELECT index_file_source_id, reporting_entity_name, reporting_entity_type, file_type, file_desc.key, file_desc.pos, file_desc_value.key as file_desc_value_key, file_desc_value.pos as file_desc_value_pos, file_desc_value.value as file_desc_value_value|>
--   SELECT * EXCEPT (key, file_desc_value_pos) |>
--   PIVOT (first(file_desc_value_value) FOR file_desc_value_key IN ('description','plan_id_type','plan_id','plan_market_type','plan_name','location')) |> 
--   SELECT index_file_source_id, reporting_entity_name, reporting_entity_type, file_type, description::string as file_description, plan_id_type::string as reporting_plan_id_type, plan_id::string as reporting_plan_id, plan_market_type::string as reporting_plan_market_type, plan_name::string as reporting_plan_name, location::string as file_location |>
--   SELECT *, sha2(concat(index_file_source_id, reporting_entity_name, reporting_entity_type, file_type, file_description, file_location), 256) as allowed_amount_file_index_id |>
--   SELECT 
--     allowed_amount_file_index_id,
--     index_file_source_id,
--     reporting_entity_name,
--     reporting_entity_type,
--     file_type,
--     file_description,
--     file_location
-- );