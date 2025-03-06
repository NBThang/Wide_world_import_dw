WITH dim_customer__source as (
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_customer__rename as (
  SELECT
    customer_id as customer_key
    , customer_name
    , customer_category_id as customer_category_key
    , buying_group_id as buying_group_key
    , is_on_credit_hold as is_on_credit_hold_boolean

    , is_statement_sent as is_statement_sent_boolean
    , credit_limit
    , standard_discount_percentage
    , payment_days
    , account_opened_date
    , delivery_method_id as delivery_method_key
    , delivery_city_id as delivery_city_key
    , postal_city_id as postal_city_key
  FROM dim_customer__source
)

, dim_customer__CastType as (
  SELECT
    CAST(customer_key as INTEGER) as customer_key
    , CAST(customer_name as STRING) as customer_name
    , CAST(customer_category_key as INTEGER) as customer_category_key
    , CAST(buying_group_key as INTEGER) as buying_group_key
    , CAST(is_on_credit_hold_boolean as BOOLEAN) as is_on_credit_hold_boolean
    , CAST(is_statement_sent_boolean as BOOLEAN) as is_statement_sent_boolean
    , CAST(credit_limit as NUMERIC) as credit_limit
    , CAST(standard_discount_percentage as NUMERIC) as standard_discount_percentage
    , CAST(payment_days as INTEGER) as payment_days
    , CAST(account_opened_date as TIMESTAMP) as account_opened_date
    , CAST(delivery_method_key as INTEGER) as delivery_method_key
    , CAST(delivery_city_key as INTEGER) as delivery_city_key
    , CAST(postal_city_key as INTEGER) as postal_city_key
  FROM dim_customer__rename
)

, dim_customer__ConvertBoolean as (
  SELECT
    customer_key
    , customer_name
    , customer_category_key
    , buying_group_key
    , CASE 
        WHEN is_on_credit_hold_boolean is TRUE THEN 'On Credit Hold'
        WHEN is_on_credit_hold_boolean is FALSE THEN 'Not On Credit Hold'
        WHEN is_on_credit_hold_boolean is NULL THEN 'Undefined'
      ELSE 'Invalid' END
      as is_on_credit_hold
    , CASE 
        WHEN is_statement_sent_boolean is TRUE THEN 'Statement Sent'
        WHEN is_statement_sent_boolean is FALSE THEN 'Not Statement Sent'
        WHEN is_statement_sent_boolean is NULL THEN 'Undefined'
      ELSE 'Invalid' END
      as is_statement_sent
    , credit_limit
    , standard_discount_percentage
    , payment_days
    , account_opened_date
    , delivery_method_key
    , delivery_city_key
    , postal_city_key
  FROM dim_customer__CastType
)

, dim_customer__add_undefined_record as (
  SELECT
    customer_key
    , customer_name
    , customer_category_key
    , buying_group_key
    , is_on_credit_hold
    , is_statement_sent
    , credit_limit
    , standard_discount_percentage
    , payment_days
    , account_opened_date
    , delivery_method_key
    , delivery_city_key
    , postal_city_key
  FROM dim_customer__ConvertBoolean

  UNION ALL
  SELECT
    0 as customer_key
    , 'Undefined' as customer_name
    , 0 as customer_category_key
    , 0 as buying_group_key
    , 'Undefined' as is_on_credit_hold
    , 'Undefined' as is_statement_sent
    , 0 as credit_limit
    , 0 as standard_discount_percentage
    , 0 as payment_days
    , '2000-01-01 00:00:00 UTC' as account_opened_date
    , 0 as delivery_method_key
    , 0 as delivery_city_key
    , 0 as postal_city_key
  FROM dim_customer__ConvertBoolean

  UNION ALL
  SELECT
    -1 as customer_key
    , 'Invalid' as customer_name
    , -1 as customer_category_key
    , -1 as buying_group_key
    , 'Invalid' as is_on_credit_hold
    , 'Invalid' as is_statement_sent
    , -1 as credit_limit
    , -1 as standard_discount_percentage
    , -1 as payment_days
    , '2000-01-01 00:00:00 UTC' as account_opened_date
    , -1 as delivery_method_key
    , -1 as delivery_city_key
    , -1 as postal_city_key
  FROM dim_customer__ConvertBoolean
)

SELECT
  dim_customer.customer_key
  , dim_customer.customer_name
  , dim_customer.customer_category_key
  , dim_customer.buying_group_key
  , dim_customer.is_on_credit_hold
  , COALESCE(dim_customer_category.customer_category_name, 'Invalid') as customer_category_name
  , COALESCE(dim_buying_group.buying_group_name, 'Invalid') as buying_group_name
  , dim_customer.is_statement_sent
  , dim_customer.credit_limit
  , dim_customer.standard_discount_percentage
  , dim_customer.payment_days
  , dim_customer.account_opened_date
  , dim_customer.delivery_method_key
  , dim_delivery.delivery_method_name
  , dim_customer.delivery_city_key
  , dim_city_province.city_name as delivery_city_name
  , dim_city_province.state_province_key as delivery_state_province_key
  , dim_city_province.state_province_name as delivery_state_province_name
  , dim_city_province.country_key as delivery_country_key
  , dim_city_province.country_name as delivery_country_name
  , dim_customer.postal_city_key
  , dim_city_postal.city_name as postal_city_name
  , dim_city_postal.state_province_key as postal_state_province_key
  , dim_city_postal.state_province_name as postal_state_province_name
  , dim_city_postal.country_key as postal_country_key
  , dim_city_postal.country_name as postal_country_name
FROM dim_customer__add_undefined_record as dim_customer
LEFT JOIN {{ref('stg_dim_customer_category')}} as dim_customer_category
  ON dim_customer.customer_category_key = dim_customer_category.customer_category_key
LEFT JOIN {{ref('stg_dim_buying_group')}} as dim_buying_group
  ON dim_customer.buying_group_key = dim_buying_group.buying_group_key
LEFT JOIN {{ref('stg_dim_delivery')}} as dim_delivery
  ON dim_customer.delivery_method_key = dim_delivery.delivery_method_key
LEFT JOIN {{ref('stg_dim_city')}} as dim_city_province
  ON dim_customer.delivery_city_key = dim_city_province.city_key
LEFT JOIN {{ref('stg_dim_city')}} as dim_city_postal
  ON dim_customer.delivery_city_key = dim_city_postal.city_key



