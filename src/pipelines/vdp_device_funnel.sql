-- Pipeline: vdp_device_funnel
-- Source: meridian_t02.silver.silver_clickstream
-- Output: meridian_t02.gold.vdp_device_funnel
-- Parameter: min_year (INT, default: 2020)

-- Step 1: Source — read clickstream events from the silver table
-- Step 2: Filter — keep rows where YEAR(event_timestamp) >= :min_year
-- Step 3: Derive — categorize funnel_step using CASE expression
-- Step 4: Aggregate — group by device_type, event, funnel_step; COUNT session_id, COUNT DISTINCT session_id and user_id
-- Step 5: Sort — order by device_type ASC, funnel_step ASC
-- Step 6: Output — write to meridian_t02.gold.vdp_device_funnel (overwrite)

CREATE OR REPLACE TABLE meridian_t02.gold.vdp_device_funnel AS
SELECT
  device_type,
  event,
  funnel_step,
  COUNT(session_id) AS total_events,
  COUNT(DISTINCT session_id) AS unique_sessions,
  COUNT(DISTINCT user_id) AS unique_users
FROM (
  SELECT
    device_type,
    event,
    session_id,
    user_id,
    CASE
      WHEN event = 'search' THEN 1
      WHEN event = 'view' THEN 2
      WHEN event IN ('booking_attempt', 'confirm_booking') THEN 3
      ELSE 0
    END AS funnel_step
  FROM meridian_t02.silver.silver_clickstream
  WHERE YEAR(event_timestamp) >= :min_year
)
GROUP BY device_type, event, funnel_step
ORDER BY device_type ASC, funnel_step ASC;