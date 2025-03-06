WITH fact_sales_order_line__source as (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order_line__rename as (
  SELECT 
    order_line_id as sales_order_line_key
    , order_id as order_key
    , stock_item_id as product_key
    , quantity
    , unit_price
  FROM fact_sales_order_line__source
)

, fact_sales_order_line__CastType as (
  SELECT
    CAST(sales_order_line_key as INTEGER) as sales_order_line_key
    , CAST(order_key as INTEGER) as order_key
    , CAST(product_key as INTEGER) as product_key
    , CAST(quantity as INTEGER) as quantity
    , CAST(unit_price as NUMERIC) as unit_price
  FROM fact_sales_order_line__rename
)

SELECT 
  fact_order_line.sales_order_line_key
  , fact_order_line.product_key
  , COALESCE(fact_order.customer_key, -1) as customer_key 
  , COALESCE(fact_order.picked_by_person_key, 0) as picked_by_person_key
  , fact_order_line.quantity
  , fact_order_line.unit_price
  , (fact_order_line.quantity * fact_order_line.unit_price) as gross_amount
  , fact_order.order_date
FROM fact_sales_order_line__CastType as fact_order_line
LEFT JOIN {{ref('stg_fact_sales_order')}} as fact_order
  ON fact_order_line.order_key = fact_order.order_key