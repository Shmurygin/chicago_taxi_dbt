{{
  config(
    materialized='table'
  )
}}

WITH monthly_tips AS (
  SELECT
    t.taxi_id,
    DATE_TRUNC(t.trip_start_timestamp, MONTH) AS year_month,
    ROUND(SUM(t.tips),2) AS tips_sum
  FROM {{ ref('stg_taxi_trips__trips') }} t
  INNER JOIN {{ ref('int_top_taxi_april') }} top
    ON t.taxi_id = top.taxi_id
  WHERE 
    trip_start_timestamp >= TIMESTAMP('2018-04-01')
  GROUP BY 
    t.taxi_id,
    year_month
  ORDER BY
    t.taxi_id,
    year_month
)

SELECT
  taxi_id,
  year_month,
  tips_sum
FROM monthly_tips
ORDER BY 
  taxi_id,
  year_month