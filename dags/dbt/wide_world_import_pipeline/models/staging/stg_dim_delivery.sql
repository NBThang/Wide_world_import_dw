WITH dim_delivery__source as (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.application__delivery_methods`
)

, dim_delivery__rename as (
  SELECT
    delivery_method_id as delivery_method_key
    , delivery_method_name
  FROM dim_delivery__source
)

, dim_delivery__CastType as (
  SELECT
    CAST(delivery_method_key as INTEGER) as delivery_method_key
    , CAST(delivery_method_name as STRING) as delivery_method_name
  FROM dim_delivery__rename
)

SELECT
  delivery_method_key
  , delivery_method_name
FROM dim_delivery__CastType