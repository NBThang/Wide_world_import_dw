WITH dim_city__source as (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.application__cities`
)

, dim_city__rename as (
  SELECT
    city_id as city_key
    , city_name
    , state_province_id as state_province_key
  FROM dim_city__source
)

, dim_city__CastType as (
  SELECT
    CAST(city_key as INTEGER) as city_key
    , CAST(city_name as STRING) as city_name
    , CAST(state_province_key as INTEGER) as state_province_key
  FROM dim_city__rename
)

SELECT
  dim_city.city_key
  , dim_city.city_name
  , dim_city.state_province_key
  , dim_state_province.state_province_name
  , dim_state_province.country_key
  , dim_state_province.country_name
FROM dim_city__CastType as dim_city
LEFT JOIN {{ref('stg_dim_state_province')}} as dim_state_province
  ON dim_city.state_province_key = dim_state_province.state_province_key