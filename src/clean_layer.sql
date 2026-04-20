
-- =============================================================================
-- clean_layer.sql
-- Transforms raw_* tables into clean_* tables.
-- Handles: nulls, type casting, deduplication, invalid values, referential integrity.
-- Run after load_to_db.py
-- =============================================================================


-- =============================================================================
-- CLEAN CUSTOMERS
-- Minimal cleaning needed — reference table, used to validate FKs downstream
-- =============================================================================
DROP TABLE IF EXISTS clean_customers;

CREATE TABLE clean_customers AS
SELECT
    customer_id,
    full_name,
    email,
    phone,
    state,
    DATE(created_at)                        AS created_at
FROM raw_customers
WHERE
    customer_id IS NOT NULL                 
    or full_name is not null
    or email is not null
    or phone is not null
    or state is not null
    or created_at is not null;


-- =============================================================================
-- CLEAN ACCOUNTS
-- Issues to fix:
--   • NULL balance_aud          → replace with 0.00
--   • Future open_date          → flag and exclude
--   • Cast balance to REAL
--   • Only keep accounts whose customer_id exists in clean_customers
-- =============================================================================
DROP TABLE IF EXISTS clean_accounts;

CREATE TABLE clean_accounts AS
SELECT
    a.account_id,
    a.customer_id,
    a.data_holder,
    a.account_type,
    COALESCE(CAST(a.balance_aud AS REAL), 0.00)  AS balance_aud,
    a.currency,
    a.status,
    DATE(a.open_date)                             AS open_date,
    DATETIME(a.last_updated)                      AS last_updated,

    -- Flag for auditability (not excluded — just labelled)
    CASE WHEN a.balance_aud IS NULL THEN 1 ELSE 0 END AS flag_null_balance
FROM raw_accounts a
INNER JOIN clean_customers c
    ON a.customer_id = c.customer_id       -- remove orphaned accounts
WHERE
    a.account_id IS NOT NULL
    AND DATE(a.open_date) <= DATE('now');  -- exclude future open dates


-- =============================================================================
-- CLEAN CONSENTS
-- Issues to fix:
--   • NULL customer_id          → exclude (can't link to a customer)
--   • Only keep valid statuses
--   • Cast dates properly
--   • Only keep consents whose customer_id exists in clean_customers
-- =============================================================================
DROP TABLE IF EXISTS clean_consents;

CREATE TABLE clean_consents AS
SELECT
    co.consent_id,
    co.customer_id,
    co.data_holder,
    co.status,
    co.scopes,
    DATETIME(co.created_at)                AS created_at,
    DATETIME(co.expires_at)                AS expires_at,
    DATETIME(co.revoked_at)                AS revoked_at,
    CAST(co.sharing_duration_days AS INTEGER) AS sharing_duration_days
FROM raw_consents co
INNER JOIN clean_customers c
    ON co.customer_id = c.customer_id      -- remove consents with no matching customer
WHERE
    co.consent_id IS NOT NULL
    AND co.status IN ('active', 'revoked', 'expired');  -- drop unknown statuses


-- =============================================================================
-- CLEAN TRANSACTIONS
-- Issues to fix:
--   • Duplicate transaction_id  → keep only the first occurrence (by rowid)
--   • Negative or zero amounts  → exclude
--   • Only keep transactions whose account_id exists in clean_accounts
--   • Cast amount to REAL
-- =============================================================================
DROP TABLE IF EXISTS clean_transactions;

CREATE TABLE clean_transactions AS
SELECT
    t.transaction_id,
    t.account_id,
    t.transaction_type,
    CAST(t.amount_aud AS REAL)             AS amount_aud,
    t.currency,
    t.merchant_name,
    DATETIME(t.transaction_date)           AS transaction_date,
    DATETIME(t.posted_date)                AS posted_date,
    t.description,
    t.status
FROM raw_transactions t
INNER JOIN clean_accounts a
    ON t.account_id = a.account_id         -- remove orphaned transactions
WHERE
    t.transaction_id IN (
        -- Deduplication: keep only the first rowid per transaction_id
        SELECT transaction_id
        FROM (
            SELECT transaction_id, MIN(rowid) AS first_rowid
            FROM raw_transactions
            GROUP BY transaction_id
        )
    )
    AND CAST(t.amount_aud AS REAL) > 0     -- exclude zero or negative amounts
    AND t.transaction_id IS NOT NULL;


-- =============================================================================
-- CLEAN FEED LOGS
-- Issues to fix:
--   • NULL latency_ms on non-failed feeds → replace with -1 as sentinel
--   • Cast numeric columns
--   • Only keep known statuses
-- =============================================================================
DROP TABLE IF EXISTS clean_feed_logs;

CREATE TABLE clean_feed_logs AS
SELECT
    feed_log_id,
    data_holder,
    feed_type,
    DATETIME(run_at)                                   AS run_at,
    status,
    CAST(records_expected AS INTEGER)                  AS records_expected,
    CAST(records_received AS INTEGER)                  AS records_received,
    COALESCE(CAST(latency_ms AS INTEGER), -1)          AS latency_ms,
    error_code,
    ingestion_version,

    -- Reconciliation variance: how many records went missing (%)
    CASE
        WHEN CAST(records_expected AS REAL) = 0 THEN NULL
        ELSE ROUND(
            (CAST(records_expected AS REAL) - CAST(records_received AS REAL))
            / CAST(records_expected AS REAL) * 100, 2
        )
    END AS variance_pct

FROM raw_feed_logs
WHERE
    feed_log_id IS NOT NULL
    AND status IN ('success', 'failed', 'partial');
