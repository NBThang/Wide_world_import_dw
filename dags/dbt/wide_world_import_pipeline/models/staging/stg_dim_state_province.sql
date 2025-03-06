WITH dim_state_province__sourse as (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.application__state_provinces`
)

, dim_state_province__rename as (
  SELECT
    state_province_id as state_province_key
    , state_province_name
    , country_id as country_key
  FROM dim_state_province__sourse
)

, dim_state_province__CastType as (
  SELECT
    CAST(state_province_key as INTEGER) as state_province_key
    , CAST(state_province_name as STRING) as state_province_name
    , CAST(country_key as INTEGER) as country_key
  FROM dim_state_province__rename
)

SELECT 
  dim_state_province.state_province_key
  , dim_state_province.state_province_name
  , dim_state_province.country_key
  , dim_country.country_name
FROM dim_state_province__CastType as dim_state_province
LEFT JOIN {{ref('stg_dim_country')}} as dim_country
  On dim_state_province.country_key = dim_country.country_key