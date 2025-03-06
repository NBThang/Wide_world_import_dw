WITH fact_sales_order__source as (
  SELECT
    * 
  FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, fact_sales_order__rename as (
  SELECT
    order_id as order_key
    , customer_id as customer_key
    , picked_by_person_id as picked_by_person_key 
    , order_date   
  FROM fact_sales_order__source
)

, fact_sales_order__CastType as (
  SELECT
    CAST(order_key as INTEGER) as order_key
    , CAST(customer_key as INTEGER) as customer_key
    , CAST(picked_by_person_key as INTEGER) as picked_by_person_key
    , CAST(order_date as DATE) as order_date
  FROM fact_sales_order__rename
)

SELECT
  order_key
  , customer_key
  , picked_by_person_key
  , order_date
FROM fact_sales_order__CastType

