WITH dim_customer_category__source as (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.sales__customer_categories`
)

, dim_customer_category__rename as (
  SELECT
    customer_category_id as customer_category_key
    , customer_category_name
  FROM dim_customer_category__source
)

, dim_customer_category__CastType as (
  SELECT
    CAST(customer_category_key as INTEGER) as customer_category_key
    , CAST(customer_category_name as STRING) as customer_category_name
  FROM dim_customer_category__rename
)

, dim_customer_category__add_undefined_record as (
  SELECT
    customer_category_key
    , customer_category_name
  FROM dim_customer_category__CastType

  UNION ALL
  SELECT
    0 as customer_category_key
    , 'Undefined' as customer_category_name
  FROM dim_customer_category__CastType

  UNION ALL
  SELECT
    -1 as customer_category_key
    , 'Invalid' as customer_category_name
  FROM dim_customer_category__CastType
)

SELECT
  dim_customer_category.customer_category_key
  , dim_customer_category.customer_category_name
FROM dim_customer_category__add_undefined_record as dim_customer_category