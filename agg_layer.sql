
-- =============================================================================
-- agg_layer.sql
-- Creates aggregated KPI tables from clean_* tables.
-- These feed directly into the Tableau dashboard.
-- Run after clean_layer.sql
-- =============================================================================


-- =============================================================================
-- AGG 1: FEED HEALTH BY DATA HOLDER AND DAY
-- Powers: Feed Health Monitor chart in Tableau
-- =============================================================================
DROP TABLE IF EXISTS agg_feed_health;

CREATE TABLE agg_feed_health AS
SELECT
    data_holder,
    feed_type,
    DATE(run_at)                                        AS feed_date,
    COUNT(*)                                            AS total_runs,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN status = 'failed'  THEN 1 ELSE 0 END) AS failed_count,
    SUM(CASE WHEN status = 'partial' THEN 1 ELSE 0 END) AS partial_count,
    ROUND(
        SUM(CASE WHEN status = 'success' THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 1
    )                                                   AS success_rate_pct,
    ROUND(AVG(CASE WHEN latency_ms > 0 THEN latency_ms END), 0)
                                                        AS avg_latency_ms,
    ROUND(MAX(CASE WHEN latency_ms > 0 THEN latency_ms END), 0)
                                                        AS max_latency_ms,
    SUM(records_expected)                               AS total_records_expected,
    SUM(records_received)                               AS total_records_received
FROM clean_feed_logs
GROUP BY
    data_holder,
    feed_type,
    DATE(run_at);


-- =============================================================================
-- AGG 2: CONSENT SUMMARY BY DATA HOLDER
-- Powers: Consent Volume Trend chart in Tableau
-- =============================================================================
DROP TABLE IF EXISTS agg_consent_summary;

CREATE TABLE agg_consent_summary AS
SELECT
    data_holder,
    DATE(created_at)                                        AS consent_date,
    COUNT(*)                                                AS total_consents,
    SUM(CASE WHEN status = 'active'  THEN 1 ELSE 0 END)    AS active_count,
    SUM(CASE WHEN status = 'revoked' THEN 1 ELSE 0 END)    AS revoked_count,
    SUM(CASE WHEN status = 'expired' THEN 1 ELSE 0 END)    AS expired_count,
    ROUND(
        SUM(CASE WHEN status = 'active' THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 1
    )                                                       AS active_rate_pct,
    ROUND(AVG(sharing_duration_days), 0)                    AS avg_sharing_days
FROM clean_consents
GROUP BY
    data_holder,
    DATE(created_at);


-- =============================================================================
-- AGG 3: RECONCILIATION VARIANCE BY DATA HOLDER
-- Powers: Reconciliation Variance chart in Tableau
-- Highlights feeds where records received fell more than 5% below expected
-- =============================================================================
DROP TABLE IF EXISTS agg_reconciliation;

CREATE TABLE agg_reconciliation AS
SELECT
    data_holder,
    feed_type,
    DATE(run_at)                                            AS feed_date,
    SUM(records_expected)                                   AS total_expected,
    SUM(records_received)                                   AS total_received,
    SUM(records_expected) - SUM(records_received)           AS total_gap,
    ROUND(
        (SUM(records_expected) - SUM(records_received)) * 1.0
        / NULLIF(SUM(records_expected), 0) * 100, 2
    )                                                       AS gap_pct,
    CASE
        WHEN (SUM(records_expected) - SUM(records_received)) * 1.0
             / NULLIF(SUM(records_expected), 0) > 0.05
        THEN 'BREACH'
        ELSE 'OK'
    END                                                     AS reconciliation_status
FROM clean_feed_logs
WHERE status IN ('partial', 'failed')
GROUP BY
    data_holder,
    feed_type,
    DATE(run_at);


-- =============================================================================
-- AGG 4: DAILY TRANSACTION VOLUMES BY ACCOUNT TYPE
-- Powers: Transaction volume trend in Tableau
-- =============================================================================
DROP TABLE IF EXISTS agg_transaction_volumes;

CREATE TABLE agg_transaction_volumes AS
SELECT
    a.account_type,
    a.data_holder,
    DATE(t.transaction_date)                                AS txn_date,
    t.transaction_type,
    COUNT(*)                                                AS txn_count,
    ROUND(SUM(t.amount_aud), 2)                             AS total_amount_aud,
    ROUND(AVG(t.amount_aud), 2)                             AS avg_amount_aud
FROM clean_transactions t
INNER JOIN clean_accounts a
    ON t.account_id = a.account_id
GROUP BY
    a.account_type,
    a.data_holder,
    DATE(t.transaction_date),
    t.transaction_type;
