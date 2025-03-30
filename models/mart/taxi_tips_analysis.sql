{{
  config(
    materialized='incremental',
    unique_key='row_id',
    incremental_strategy='merge',
    merge_update_columns=['tips_sum', 'tips_change']
  )
}}
--считаем, что новые данные не могут добавить в источник, только измениться старые
WITH monthly_data AS (
  SELECT
    CONCAT(taxi_id, '_', FORMAT_DATE('%Y-%m', year_month)) AS row_id,
    taxi_id,
    FORMAT_DATE('%Y-%m', year_month) as year_month,
    tips_sum,
    LAG(tips_sum) OVER (PARTITION BY taxi_id ORDER BY year_month) AS prev_month_tips
  FROM {{ ref('int_monthly_tips') }}
)

SELECT
  --row_id,
  taxi_id,
  year_month,
  tips_sum,
  CASE 
    WHEN prev_month_tips IS NULL THEN NULL
    WHEN prev_month_tips = 0 THEN NULL
    ELSE ROUND(((tips_sum - prev_month_tips) / prev_month_tips) * 100, 2)
  END AS `percent`,
  CURRENT_TIMESTAMP() AS updated_at
FROM monthly_data
{% if is_incremental() %}
WHERE row_id IN (
  -- Выбираем только те записи, где изменилась сумма чаевых
  SELECT CONCAT(taxi_id, '_', FORMAT_DATE('%Y-%m', year_month))
  FROM {{ ref('int_monthly_tips') }}
  WHERE tips_sum != (
    SELECT tips_sum 
    FROM {{ this }} 
    WHERE {{ this }}.row_id = CONCAT(taxi_id, '_', FORMAT_DATE('%Y-%m', year_month))
  )
 
)
{% endif %}