-- ============================================================
-- GA4 Mass Balance Reconciliation
-- Principle: Revenue_In = Revenue_Out + Unattributed_Delta
-- If delta > threshold → tracking leak detected
-- Author: Charles Aniji | GA4 Data Integrity Auditor
-- ============================================================

WITH shopify_revenue AS (
  -- Source of truth: Shopify transaction log
  -- Replace with your actual Shopify export table
  SELECT
    -- PRO TIP: Normalize Shopify UTC timestamp to match GA4 Property Timezone
    DATE(DATETIME(created_at, 'America/New_York')) AS transaction_date,
    SUM(total_price)                               AS shopify_gross_revenue,
    COUNT(DISTINCT order_id)                       AS shopify_order_count
  FROM `your_project.shopify_exports.orders`
  WHERE financial_status IN ('paid', 'partially_refunded')
  GROUP BY 1
),

ga4_revenue AS (
  -- GA4 purchase events from BigQuery export
  -- event_date is already in the GA4 property timezone (set in GA4 Admin)
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS transaction_date,
    SUM(ecommerce.purchase_revenue)  AS ga4_reported_revenue,
    COUNT(DISTINCT ecommerce.transaction_id) AS ga4_order_count
  FROM `your_project.analytics_XXXXXX.events_*`
  WHERE event_name = 'purchase'
  GROUP BY 1
),

reconciliation AS (
  SELECT
    s.transaction_date,
    s.shopify_gross_revenue,
    g.ga4_reported_revenue,
    s.shopify_order_count,
    g.ga4_order_count,

    -- The Delta: this is your "mass balance discrepancy"
    (s.shopify_gross_revenue - COALESCE(g.ga4_reported_revenue, 0)) AS revenue_delta,
    (s.shopify_order_count   - COALESCE(g.ga4_order_count, 0))      AS order_delta,

    -- Leak rate as percentage
    ROUND(
      SAFE_DIVIDE(
        s.shopify_gross_revenue - COALESCE(g.ga4_reported_revenue, 0),
        s.shopify_gross_revenue
      ) * 100, 2
    ) AS leak_rate_pct,

    -- Safety flag: >5% delta triggers a CRITICAL alert
    CASE
      WHEN SAFE_DIVIDE(
        s.shopify_gross_revenue - COALESCE(g.ga4_reported_revenue, 0),
        s.shopify_gross_revenue
      ) > 0.05 THEN 'CRITICAL — Tracking Leak Detected'
      WHEN SAFE_DIVIDE(
        s.shopify_gross_revenue - COALESCE(g.ga4_reported_revenue, 0),
        s.shopify_gross_revenue
      ) BETWEEN 0.02 AND 0.05 THEN 'WARN — Review GTM Configuration'
      ELSE 'PASS'
    END AS integrity_status

  FROM shopify_revenue s
  LEFT JOIN ga4_revenue g USING (transaction_date)
)

SELECT * FROM reconciliation
ORDER BY transaction_date DESC;
