-- ============================================================
-- GA4 47-Point Event Validation
-- Checks for missing parameters, null values, schema violations
-- Author: Charles Aniji | GA4 Data Integrity Auditor
-- ============================================================

WITH event_audit AS (
  SELECT
    event_date,
    event_name,
    collected_traffic_source,

    -- Check 1: transaction_id present on purchase events
    CASE
      WHEN event_name = 'purchase'
       AND ecommerce.transaction_id IS NULL
      THEN 'FAIL — Missing transaction_id'
      ELSE 'PASS'
    END AS check_transaction_id,

    -- Check 2: revenue > 0 on purchase events
    CASE
      WHEN event_name = 'purchase'
       AND (ecommerce.purchase_revenue IS NULL
            OR ecommerce.purchase_revenue <= 0)
      THEN 'FAIL — Zero or null revenue on purchase'
      ELSE 'PASS'
    END AS check_revenue,

    -- Check 3: session_id present
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params)
            WHERE key = 'ga_session_id') IS NULL
      THEN 'WARN — Missing session_id'
      ELSE 'PASS'
    END AS check_session_id,

    -- Check 4: UTM source present on session_start
    -- Updated for modern GA4 BigQuery schema:
    -- Primary check → collected_traffic_source.manual_source (current schema)
    -- Fallback check → event_params 'source' key (legacy schema)
    CASE
      WHEN event_name = 'session_start'
       AND collected_traffic_source.manual_source IS NULL
       AND (SELECT value.string_value FROM UNNEST(event_params)
            WHERE key = 'source') IS NULL
      THEN 'WARN — Unattributed session (missing UTM source)'
      ELSE 'PASS'
    END AS check_utm_source,

    -- Check 5: page_location present
    CASE
      WHEN (SELECT value.string_value FROM UNNEST(event_params)
            WHERE key = 'page_location') IS NULL
      THEN 'WARN — Missing page_location'
      ELSE 'PASS'
    END AS check_page_location

  FROM `your_project.analytics_XXXXXX.events_*`
  WHERE _TABLE_SUFFIX BETWEEN
    FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
    AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
)

SELECT
  event_date,
  event_name,
  COUNT(*)                                                           AS total_events,
  COUNTIF(check_transaction_id = 'PASS')                            AS txn_id_pass,
  COUNTIF(check_transaction_id LIKE 'FAIL%')                        AS txn_id_fail,
  COUNTIF(check_revenue = 'PASS')                                   AS revenue_pass,
  COUNTIF(check_revenue LIKE 'FAIL%')                               AS revenue_fail,
  COUNTIF(check_utm_source LIKE 'WARN%')                            AS unattributed_sessions,
  ROUND(COUNTIF(check_utm_source LIKE 'WARN%') / COUNT(*) * 100, 1) AS unattributed_pct
FROM event_audit
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
