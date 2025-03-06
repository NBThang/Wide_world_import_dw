WITH dim_date__source AS (
  SELECT 
    *
  FROM UNNEST(GENERATE_DATE_ARRAY('2014-01-01', '2030-01-01', INTERVAL 1 DAY)) AS date
)

SELECT
  date
  , FORMAT_DATE('%A', date) AS day_of_week
  , FORMAT_DATE('%a', date) AS day_of_week_short
  , CASE 
      WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN   'Weekend'  -- Chủ Nhật (1) và Thứ Bảy (7)
      ELSE 'Weekday'  -- Thứ Hai đến Thứ Sáu (2-6)
    END AS is_weekday_or_weekend
  , DATE_TRUNC(date, MONTH) as year_month
  , FORMAT_DATE('%B', date) as month
  , DATE_TRUNC(date, YEAR) as year
  , CAST(FORMAT_DATE('%Y', date) as INTEGER) as year_number
FROM dim_date__source
