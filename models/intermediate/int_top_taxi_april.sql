{{
  config(
    materialized='table'
  )
}}
--сгруппировали для 2018.04 и взяли топ 3
WITH april_tips AS (
  SELECT
    taxi_id,
    SUM(tips) AS tips_sum
  FROM {{ ref('stg_taxi_trips__trips') }}
  WHERE 
    EXTRACT(YEAR FROM trip_start_timestamp) = 2018
    AND EXTRACT(MONTH FROM trip_start_timestamp) = 4
  GROUP BY taxi_id
  ORDER BY tips_sum DESC
  LIMIT 3
)

SELECT 
  taxi_id,
  ROUND(tips_sum,2) AS april_tips_sum
FROM april_tips