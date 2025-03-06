WITH dim_supplier__source as (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, dim_supplier__rename as (
  SELECT
    supplier_id as supplier_key
    , supplier_name
  FROM dim_supplier__source
)

, dim_supplier__CastType as (
  SELECT 
    CAST(supplier_key as INTEGER) as supplier_key
    , CAST(supplier_name as STRING) as supplier_name
  FROM dim_supplier__rename
)

, dim_supplier__add_undefined_record as (
  SELECT
    supplier_key
    , supplier_name
  FROM dim_supplier__CastType

  UNION ALL
  SELECT
    0 as supplier_key
    , 'Undefined' as supplier_name
  FROM dim_supplier__CastType

  UNION ALL
  SELECT
    -1 as supplier_key
    , 'Invalid' as supplier_name
  FROM dim_supplier__CastType
)

SELECT
  dim_supplier.supplier_key
  , dim_supplier.supplier_name
FROM dim_supplier__add_undefined_record as dim_supplier
