WITH dim_buying_group__source as (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.sales__buying_groups`
)

, dim_buying_group__rename as (
  SELECT
    buying_group_id as buying_group_key
    , buying_group_name
  FROM dim_buying_group__source
)

, dim_buying_group__CastType as (
  SELECT
    CAST(buying_group_key as INTEGER) as buying_group_key
    , CAST(buying_group_name as STRING) as buying_group_name
  FROM dim_buying_group__rename
)

, dim_buying_group__add_undefined_record as (
  SELECT
    buying_group_key
    , buying_group_name
  FROM dim_buying_group__CastType

  UNION ALL
  SELECT
    0 as buying_group_key
    , 'Undefined'buying_group_name
  FROM dim_buying_group__CastType

  UNION ALL
  SELECT
    -1 as buying_group_key
    , 'Invalid' as buying_group_name
  FROM dim_buying_group__CastType
)

SELECT
  dim_buying_group.buying_group_key
  , dim_buying_group.buying_group_name
FROM dim_buying_group__add_undefined_record as dim_buying_group
