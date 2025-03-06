WITH dim_person__source as (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.application__people`
)

, dim_person__rename as (
  SELECT
    person_id as person_key
    , full_name
  FROM dim_person__source
)

, dim_person__CastType as (
  SELECT 
    CAST(person_key as INTEGER) as person_key
    , CAST(full_name as STRING) as full_name
  FROM dim_person__rename
)

SELECT 
  person_key
  , full_name
FROM dim_person__CastType

UNION ALL
SELECT
  0 as person_key
  , 'Undefined' as full_name

