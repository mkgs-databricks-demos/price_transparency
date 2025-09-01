CREATE OR REFRESH STREAMING TABLE in_network_file_index (
  CONSTRAINT valid_file_type EXPECT (file_type = 'in_network_files')
  ,CONSTRAINT location_not_null EXPECT (location IS NOT NULL)
)
TBLPROPERTIES (
   'delta.autoOptimize.optimizeWrite' = 'true'
  ,'delta.autoOptimize.autoCompact' = 'true'
  ,'delta.enableChangeDataFeed' = 'true'
  ,'delta.enableDeletionVectors' = 'true'
  ,'delta.enableRowTracking' = 'true'
)
AS SELECT * FROM (
  (
    FROM (
      FROM (
        FROM index_json_bronze |> 
        SELECT file_metadata, ingest_time, try_parse_json(value) as variant_col
      ) 
      ,LATERAL variant_explode(variant_col) |>
      SELECT file_metadata, ingest_time, key, value |>
      PIVOT (first(value) FOR key IN ('reporting_entity_name', 'reporting_entity_type', 'reporting_structure')) |>
      SELECT file_metadata, ingest_time, reporting_entity_name::string, reporting_entity_type::string, reporting_structure
    )
    ,LATERAL variant_explode(reporting_structure) as reporting_structure
    ,LATERAL variant_explode(reporting_structure.value) as reporting_structure_value |>
    SELECT file_metadata, ingest_time, reporting_entity_name, reporting_entity_type, reporting_structure_value.key as file_type, reporting_structure_value.value
  )
  ,LATERAL variant_explode(value) as file_desc
  ,LATERAL variant_explode(file_desc.value) as file_desc_value |>
  WHERE file_type like 'in_network%' |>
  SELECT file_metadata, ingest_time, reporting_entity_name, reporting_entity_type, file_type, file_desc.key, file_desc.pos, file_desc_value.key as file_desc_value_key, file_desc_value.pos as file_desc_value_pos, file_desc_value.value as file_desc_value_value|>
  SELECT * EXCEPT (key, file_desc_value_pos) |>
  PIVOT (first(file_desc_value_value) FOR file_desc_value_key IN ('description','plan_id_type','plan_id','plan_market_type','plan_name','location')) |> 
  select 
    file_metadata
    ,ingest_time
    ,reporting_entity_name
    ,reporting_entity_type
    ,file_type
    ,description::string
    -- ,plan_id_type::string
    -- ,plan_id::string
    -- ,plan_market_type::string
    -- ,plan_name::string
    ,location::string
);


CREATE OR REFRESH STREAMING TABLE allowed_amount_file_index (
  CONSTRAINT valid_file_type EXPECT (file_type = 'allowed_amount_file')
  ,CONSTRAINT location_not_null EXPECT (location IS NOT NULL)
)
TBLPROPERTIES (
   'delta.autoOptimize.optimizeWrite' = 'true'
  ,'delta.autoOptimize.autoCompact' = 'true'
  ,'delta.enableChangeDataFeed' = 'true'
  ,'delta.enableDeletionVectors' = 'true'
  ,'delta.enableRowTracking' = 'true'
)
AS SELECT * FROM (
  (
    FROM (
      FROM (
        FROM index_json_bronze |> 
        SELECT file_metadata, ingest_time, try_parse_json(value) as variant_col
      ) 
      ,LATERAL variant_explode(variant_col) |>
      SELECT file_metadata, ingest_time, key, value |>
      PIVOT (first(value) FOR key IN ('reporting_entity_name', 'reporting_entity_type', 'reporting_structure')) |>
      SELECT file_metadata, ingest_time, reporting_entity_name::string, reporting_entity_type::string, reporting_structure
    )
    ,LATERAL variant_explode(reporting_structure) as reporting_structure
    ,LATERAL variant_explode(reporting_structure.value) as reporting_structure_value |>
    SELECT file_metadata, ingest_time, reporting_entity_name, reporting_entity_type, reporting_structure_value.key as file_type, reporting_structure_value.value
  )
  ,LATERAL variant_explode(value) as file_desc
  ,LATERAL variant_explode(file_desc.value) as file_desc_value |>
  WHERE file_type like 'allowed_amount%' |>
  SELECT file_metadata, ingest_time, reporting_entity_name, reporting_entity_type, file_type, file_desc.key, file_desc.pos, file_desc_value.key as file_desc_value_key, file_desc_value.pos as file_desc_value_pos, file_desc_value.value as file_desc_value_value|>
  SELECT * EXCEPT (key, file_desc_value_pos) |>
  PIVOT (first(file_desc_value_value) FOR file_desc_value_key IN ('description','plan_id_type','plan_id','plan_market_type','plan_name','location')) |> 
  select 
    file_metadata
    ,ingest_time
    ,reporting_entity_name
    ,reporting_entity_type
    ,file_type
    ,description::string
    -- ,plan_id_type::string
    -- ,plan_id::string
    -- ,plan_market_type::string
    -- ,plan_name::string
    ,location::string
);



CREATE OR REFRESH STREAMING TABLE reporting_plans (
  CONSTRAINT valid_file_type EXPECT (file_type = 'reporting_plans')
  ,CONSTRAINT location_not_null EXPECT (location IS NOT NULL)
)
TBLPROPERTIES (
   'delta.autoOptimize.optimizeWrite' = 'true'
  ,'delta.autoOptimize.autoCompact' = 'true'
  ,'delta.enableChangeDataFeed' = 'true'
  ,'delta.enableDeletionVectors' = 'true'
  ,'delta.enableRowTracking' = 'true'
)
AS SELECT * FROM (
  (
    FROM (
      FROM (
        FROM index_json_bronze |> 
        SELECT file_metadata, ingest_time, try_parse_json(value) as variant_col
      ) 
      ,LATERAL variant_explode(variant_col) |>
      SELECT file_metadata, ingest_time, key, value |>
      PIVOT (first(value) FOR key IN ('reporting_entity_name', 'reporting_entity_type', 'reporting_structure')) |>
      SELECT file_metadata, ingest_time, reporting_entity_name::string, reporting_entity_type::string, reporting_structure
    )
    ,LATERAL variant_explode(reporting_structure) as reporting_structure
    ,LATERAL variant_explode(reporting_structure.value) as reporting_structure_value |>
    SELECT file_metadata, ingest_time, reporting_entity_name, reporting_entity_type, reporting_structure_value.key as file_type, reporting_structure_value.value
  )
  ,LATERAL variant_explode(value) as file_desc
  ,LATERAL variant_explode(file_desc.value) as file_desc_value |>
  WHERE file_type like 'reporting_plan%' |>
  SELECT file_metadata, ingest_time, reporting_entity_name, reporting_entity_type, file_type, file_desc.key, file_desc.pos, file_desc_value.key as file_desc_value_key, file_desc_value.pos as file_desc_value_pos, file_desc_value.value as file_desc_value_value|>
  SELECT * EXCEPT (key, file_desc_value_pos) |>
  PIVOT (first(file_desc_value_value) FOR file_desc_value_key IN ('description','plan_id_type','plan_id','plan_market_type','plan_name','location')) |> 
  select 
    file_metadata
    ,ingest_time
    ,reporting_entity_name
    ,reporting_entity_type
    ,file_type
    ,description::string
    ,plan_id_type::string
    ,plan_id::string
    ,plan_market_type::string
    ,plan_name::string
    ,location::string
);
 