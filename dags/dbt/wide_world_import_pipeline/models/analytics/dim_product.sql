WITH dim_product__source AS (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

, dim_product__rename AS (
  SELECT
  stock_item_id as product_key
  , stock_item_name as product_name
  , brand as brand_name
  , supplier_id as supplier_key
  , is_chiller_stock as is_chiller_stock_boolean
  FROM dim_product__source
)

, dim_product__CastType AS (
  SELECT 
    CAST(product_key as INTEGER) as product_key
    , CAST(product_name as STRING) as product_name
    , CAST(brand_name as STRING) as brand_name
    , CAST(supplier_key as INTEGER) as supplier_key
    , CAST(is_chiller_stock_boolean as BOOLEAN) as is_chiller_stock_boolean
  FROM dim_product__rename
)

, dim_product__convert_boolean as (
  SELECT
    product_key
    , product_name
    , brand_name
    , supplier_key

    , CASE
        WHEN is_chiller_stock_boolean is TRUE THEN 'Chiller Stock'
        WHEN is_chiller_stock_boolean is FALSE THEN 'Not Chiller Stock'
        WHEN is_chiller_stock_boolean is NULL THEN 'Undefined'
      ELSE 'Invalid' END
    as is_chiller_stock

  FROM dim_product__CastType
)

, dim_product__add_undefined_record as (
  SELECT
    product_key
    , product_name
    , brand_name
    , supplier_key
    , is_chiller_stock
  FROM dim_product__convert_boolean

  UNION ALL
  SELECT
    0 as product_key
    , 'Undefined' as product_name
    , 'Undefined' as brand_name
    , 0 as supplier_key
    , 'Undefined' as is_chiller_stock
  FROM dim_product__convert_boolean

  UNION ALL
  SELECT
    -1 as product_key
    , 'Invalid' as product_name
    , 'Invalid' as brand_name
    , -1 as supplier_key
    , '-1' as is_chiller_stock
  FROM dim_product__convert_boolean
)

SELECT 
  dim_product.product_key
  , dim_product.product_name
  , COALESCE(dim_product.brand_name, 'Undefined') as brand_name
  , dim_product.supplier_key
  , dim_product.is_chiller_stock
  , COALESCE(dim_supplier.supplier_name, 'Invalid') as supplier_name
FROM dim_product__add_undefined_record as dim_product
LEFT JOIN {{ref('dim_supplier')}}
  ON dim_product.supplier_key = dim_supplier.supplier_key
