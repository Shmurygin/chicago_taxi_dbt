{{
  config(
    materialized='view',
    alias='stg_taxi_trips',
    cluster_by=['taxi_id', 'date_trunc(trip_start_timestamp, MONTH)']
  )
}}

SELECT
  unique_key,
  taxi_id,
  CAST(trip_start_timestamp AS TIMESTAMP) AS trip_start_timestamp,
  CAST(tips AS FLOAT64) AS tips
FROM {{ source('chicago_taxi', 'taxi_trips') }}
WHERE 
  tips IS NOT NULL
  AND taxi_id IS NOT NULL
  AND trip_start_timestamp IS NOT NULL
  AND tips > 0