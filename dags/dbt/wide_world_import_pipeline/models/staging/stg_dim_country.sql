WITH dim_country__source as (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.application__countries`
)

, dim_country__rename as (
  SELECT
    country_id as country_key
    , country_name
  FROM dim_country__source
)

, dim_country__CastType as (
  SELECT
    CAST(country_key as INTEGER) as country_key
    , CAST(country_name as STRING) as country_name
  FROM dim_country__rename
)

SELECT
  country_key
  , country_name
FROM dim_country__CastType