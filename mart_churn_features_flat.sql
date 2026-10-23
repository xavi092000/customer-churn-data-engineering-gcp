-- EXECUTION REQUIREMENTS
-- Replace PROJECT_ID, RAW_DATASET and MART_DATASET with your identifiers.
-- The raw and mart datasets must exist in the same BigQuery location.
-- Supply named query parameters: start_month DATE, end_month DATE,
-- and sources_ready BOOL. Run the full script outside session mode.
-- Use UTC calendar months; both month parameters are inclusive.
-- Retain complete row versions for profiles, invoices, contracts and support,
-- plus order history, with ingestion-added _ingested_at TIMESTAMP.
-- Preserve each version's original _ingested_at when replaying deliveries.
-- Inventory and geography require _snapshot_date DATE and complete snapshots
-- available by their observation cutoff; ingestion must enforce eligibility.
-- Usage input must contain each additive event only once. Duplicate delivery
-- prevention is an ingestion responsibility; SUM does not deduplicate events.
-- Airflow must confirm deliveries before setting sources_ready to TRUE and
-- prevent concurrent publications over overlapping month ranges.

-- DATE parameters supplied at execution time.
-- Each value must be the first day of its month.
DECLARE start_month DATE DEFAULT @start_month;
DECLARE end_month   DATE DEFAULT @end_month;

-- SOURCE_READINESS_CHECK
-- Parameter supplied by Airflow after verifying source deliveries.
DECLARE sources_ready BOOL DEFAULT @sources_ready;

ASSERT sources_ready IS TRUE
AS 'Source deliveries not confirmed for the requested period';


-- PERIOD_CHECKS
ASSERT start_month IS NOT NULL AND end_month IS NOT NULL
AS 'Both months must be provided';

ASSERT start_month = DATE_TRUNC(start_month, MONTH)
AND end_month = DATE_TRUNC(end_month, MONTH)
AS 'Parameters must be first days of months';

ASSERT start_month <= end_month
AS 'Start month must precede or equal end month';


-- VERSION_CONFLICT_CHECKS
ASSERT (
SELECT COUNT(*) = 0
FROM (
SELECT
t.CUSTOMER_ID,
t._ingested_at
FROM `PROJECT_ID.RAW_DATASET.raw_customer_profiles` AS t
WHERE DATE(t._ingested_at)
< DATE_ADD(end_month, INTERVAL 1 MONTH)
GROUP BY t.CUSTOMER_ID, t._ingested_at
HAVING COUNT(DISTINCT TO_JSON_STRING(STRUCT(
t.CUSTOMER_SINCE_DT,
t.CUSTOMER_TYPE_CD,
t.CREDIT_SCORE_VAL
))) > 1
)
) AS 'Conflicting versions: raw_customer_profiles';

ASSERT (
SELECT COUNT(*) = 0
FROM (
SELECT
t.INVOICE_ID,
t._ingested_at
FROM `PROJECT_ID.RAW_DATASET.raw_billing_invoices` AS t
WHERE DATE(t._ingested_at)
< DATE_ADD(end_month, INTERVAL 1 MONTH)
GROUP BY t.INVOICE_ID, t._ingested_at
HAVING COUNT(DISTINCT TO_JSON_STRING(STRUCT(
t.CUSTOMER_ID,
t.PAYMENT_DUE_DT,
t.PAYMENT_RECEIVED_DT,
t.INVOICE_STATUS_CD
))) > 1
)
) AS 'Conflicting versions: raw_billing_invoices';

ASSERT (
SELECT COUNT(*) = 0
FROM (
SELECT
t.CONTRACT_ID,
t._ingested_at
FROM `PROJECT_ID.RAW_DATASET.raw_product_subscriptions` AS t
WHERE DATE(t._ingested_at)
< DATE_ADD(end_month, INTERVAL 1 MONTH)
GROUP BY t.CONTRACT_ID, t._ingested_at
HAVING COUNT(DISTINCT TO_JSON_STRING(STRUCT(
t.CUSTOMER_ID,
t.EFFECTIVE_START_DT,
t.EFFECTIVE_END_DT
))) > 1
)
) AS 'Conflicting versions: raw_product_subscriptions';

ASSERT (
SELECT COUNT(*) = 0
FROM (
SELECT
t.TICKET_ID,
t._ingested_at
FROM `PROJECT_ID.RAW_DATASET.raw_support_interactions` AS t
WHERE DATE(t._ingested_at)
< DATE_ADD(end_month, INTERVAL 1 MONTH)
GROUP BY t.TICKET_ID, t._ingested_at
HAVING COUNT(DISTINCT TO_JSON_STRING(STRUCT(
t.CUSTOMER_ID,
t.INTERACTION_START_TS,
t.INTERACTION_REASON_CD,
t.FIRST_CALL_RESOLUTION_IND,
t.AGENT_SATISFACTION_SCORE
))) > 1
)
) AS 'Conflicting versions: raw_support_interactions';

-- Convention: billing_month is a calendar month, not the individual
-- billing cycle defined by BILL_PERIOD_START_DT.
-- Monthly calendar for the requested period.
CREATE TEMP TABLE months AS
SELECT
billing_month,
LAST_DAY(billing_month) AS month_end
FROM UNNEST(
GENERATE_DATE_ARRAY(start_month, end_month, INTERVAL 1 MONTH)
) AS billing_month;

-- Derive profile availability before deciding which customer-months exist.
-- Prerequisite: retained versions with stable ingestion-added timestamps.
CREATE TEMP TABLE profile_version_intervals AS
WITH distinct_versions AS (
SELECT DISTINCT
CUSTOMER_ID AS customer_id,
DATE(CUSTOMER_SINCE_DT) AS customer_since_date,
CUSTOMER_TYPE_CD AS customer_type,
CREDIT_SCORE_VAL AS credit_score,
_ingested_at AS known_from
FROM `PROJECT_ID.RAW_DATASET.raw_customer_profiles`
WHERE _ingested_at
< TIMESTAMP(DATE_ADD(end_month, INTERVAL 1 MONTH))
)
SELECT
*,
LEAD(known_from) OVER (
PARTITION BY customer_id
ORDER BY known_from
) AS known_until
FROM distinct_versions;

-- One row per customer and month, based only on the profile known then.
-- A customer first observed later does not appear in earlier months.
-- Unknown sign-up dates cannot establish eligibility and are excluded.
CREATE TEMP TABLE customer_months AS
SELECT
p.customer_id,
m.billing_month,
m.month_end,
p.customer_since_date
FROM months AS m
JOIN profile_version_intervals AS p
ON DATE(p.known_from) <= m.month_end
AND (
p.known_until IS NULL
OR DATE(p.known_until) > m.month_end
)
AND p.customer_since_date <= m.month_end
WHERE p.customer_id IS NOT NULL;

-- Monthly total of recorded data usage volumes.
CREATE TEMP TABLE usage_monthly AS
SELECT
CUSTOMER_ID AS customer_id,
DATE_TRUNC(DATE(USAGE_DT), MONTH) AS billing_month,
SUM(DATA_USAGE_GB) AS monthly_data_usage_gb
FROM `PROJECT_ID.RAW_DATASET.raw_daily_usage_metrics`
WHERE DATE(USAGE_DT) >= start_month
AND DATE(USAGE_DT) < DATE_ADD(end_month, INTERVAL 1 MONTH)
GROUP BY customer_id, billing_month;

-- Profile known at each month-end.
CREATE TEMP TABLE profile_monthly AS
SELECT
cm.customer_id,
cm.billing_month,
p.customer_type,
p.credit_score
FROM customer_months AS cm
LEFT JOIN profile_version_intervals AS p
ON cm.customer_id = p.customer_id
AND DATE(p.known_from) <= cm.month_end
AND (
p.known_until IS NULL
OR DATE(p.known_until) > cm.month_end
);

-- Latest available version of each invoice at month-end.
-- Prerequisite: versions retained with ingestion-added _ingested_at.
-- Remove identical copies across the selected columns.
-- Conflicting versions are blocked by the preceding checks.
CREATE TEMP TABLE invoice_version_intervals AS
WITH distinct_versions AS (
SELECT DISTINCT
CUSTOMER_ID AS customer_id,
INVOICE_ID AS invoice_id,
DATE(PAYMENT_DUE_DT) AS payment_due_date,
DATE(PAYMENT_RECEIVED_DT) AS payment_received_date,
INVOICE_STATUS_CD AS invoice_status,
_ingested_at AS known_from
FROM `PROJECT_ID.RAW_DATASET.raw_billing_invoices`
WHERE _ingested_at
< TIMESTAMP(DATE_ADD(end_month, INTERVAL 1 MONTH))
)
SELECT
*,
LEAD(known_from) OVER (
PARTITION BY customer_id, invoice_id
ORDER BY known_from
) AS known_until
FROM distinct_versions;

-- Keep only the version applicable at each month-end.
-- DATE(TIMESTAMP) preserves the UTC convention used in the preceding block.
CREATE TEMP TABLE invoices_month_end AS
SELECT
cm.customer_id,
cm.billing_month,
cm.month_end,
i.invoice_id,
i.payment_due_date,
i.payment_received_date,
i.invoice_status
FROM customer_months AS cm
JOIN invoice_version_intervals AS i
ON cm.customer_id = i.customer_id
AND DATE(i.known_from) <= cm.month_end
AND (
i.known_until IS NULL
OR DATE(i.known_until) > cm.month_end
);

-- Payment indicators per customer and month.
CREATE TEMP TABLE payments_monthly AS
WITH invoice_flags AS (
SELECT
customer_id,
billing_month,
CASE
WHEN invoice_status = 'UNPAID'
AND payment_received_date IS NULL
THEN FALSE
ELSE (
payment_received_date BETWEEN billing_month AND month_end
AND payment_received_date > payment_due_date
)
END AS late_payment,
CASE
WHEN invoice_status = 'PAID' THEN FALSE
WHEN invoice_status IN ('UNPAID', 'PARTIAL')
AND payment_due_date IS NOT NULL
THEN payment_due_date <= month_end
ELSE NULL
END AS overdue_invoice
FROM invoices_month_end
)
SELECT
customer_id,
billing_month,
CASE
WHEN COUNTIF(late_payment = TRUE) > 0 THEN TRUE
WHEN COUNTIF(late_payment IS NULL) > 0 THEN NULL
ELSE FALSE
END AS has_late_payment_in_month,
CASE
WHEN COUNTIF(overdue_invoice = TRUE) > 0 THEN TRUE
WHEN COUNTIF(overdue_invoice IS NULL) > 0 THEN NULL
ELSE FALSE
END AS has_overdue_invoice_at_month_end
FROM invoice_flags
GROUP BY customer_id, billing_month;


-- Tickets opened during the month, classified by reason.
-- Ticket versions known at month-end.
-- Prerequisite: versions retained with _ingested_at.
-- Derive availability intervals before filtering by ticket date.
-- The preceding checks block conflicting versions.
CREATE TEMP TABLE support_version_intervals AS
WITH distinct_versions AS (
SELECT DISTINCT
CUSTOMER_ID AS customer_id,
TICKET_ID AS ticket_id,
DATE(INTERACTION_START_TS) AS interaction_date,
INTERACTION_REASON_CD AS interaction_reason,
FIRST_CALL_RESOLUTION_IND AS first_contact_resolved,
AGENT_SATISFACTION_SCORE AS satisfaction_score,
_ingested_at AS known_from
FROM `PROJECT_ID.RAW_DATASET.raw_support_interactions`
WHERE TICKET_ID IS NOT NULL
AND _ingested_at
< TIMESTAMP(DATE_ADD(end_month, INTERVAL 1 MONTH))
)
SELECT
*,
LEAD(known_from) OVER (
PARTITION BY customer_id, ticket_id
ORDER BY known_from
) AS known_until
FROM distinct_versions;

-- Version known at month-end for tickets started during that month.
CREATE TEMP TABLE support_versions_month_end AS
SELECT
cm.customer_id,
cm.billing_month,
cm.month_end,
s.ticket_id,
s.interaction_date,
s.interaction_reason,
s.first_contact_resolved,
s.satisfaction_score
FROM customer_months AS cm
JOIN support_version_intervals AS s
ON cm.customer_id = s.customer_id
AND DATE(s.known_from) <= cm.month_end
AND (
s.known_until IS NULL
OR DATE(s.known_until) > cm.month_end
)
AND s.interaction_date BETWEEN cm.billing_month AND cm.month_end;

-- Count tickets started during the observed month.
CREATE TEMP TABLE support_counts_monthly AS
SELECT
customer_id,
billing_month,
COUNTIF(interaction_reason = 'BILLING') AS billing_ticket_count,
COUNTIF(interaction_reason = 'TECH_SUPPORT') AS technical_ticket_count
FROM support_versions_month_end
WHERE interaction_date BETWEEN billing_month AND month_end
GROUP BY customer_id, billing_month;

-- Resolution and satisfaction for the same tickets.
-- Convention: 0 means not resolved on the first contact.
CREATE TEMP TABLE support_quality_monthly AS
SELECT
customer_id,
billing_month,
COUNTIF(
first_contact_resolved = 0
) AS not_resolved_first_contact_count,
AVG(satisfaction_score) AS average_satisfaction_score,
COUNT(satisfaction_score) AS satisfaction_rating_count
FROM support_versions_month_end
WHERE interaction_date BETWEEN billing_month AND month_end
GROUP BY customer_id, billing_month;

-- Requests first observed as failed during the month.
-- Prerequisite: retained versions and ingestion-added _ingested_at
-- metadata of type TIMESTAMP.
CREATE TEMP TABLE failed_requests_monthly AS
WITH first_observed_failures AS (
SELECT
CUSTOMER_ID AS customer_id,
SMART_REQUEST_ID AS request_id,
MIN(_ingested_at) AS first_failure_observed_at
FROM `PROJECT_ID.RAW_DATASET.raw_order_v2`
WHERE REQUEST_STATUS = 'FAILED'
-- REQUESTS_END_CUTOFF
AND _ingested_at
< TIMESTAMP(DATE_ADD(end_month, INTERVAL 1 MONTH))
AND REQUEST_TYPE <> 'CANCEL'
AND SMART_REQUEST_ID IS NOT NULL
GROUP BY CUSTOMER_ID, SMART_REQUEST_ID
)
SELECT
customer_id,
DATE_TRUNC(
DATE(first_failure_observed_at),
MONTH
) AS billing_month,
COUNT(*) AS failed_request_count
FROM first_observed_failures
WHERE DATE(first_failure_observed_at) >= start_month
AND DATE(first_failure_observed_at)
< DATE_ADD(end_month, INTERVAL 1 MONTH)
GROUP BY customer_id, billing_month;

-- Prerequisite: _snapshot_date identifies each complete snapshot.
-- Use only fully loaded snapshots.

-- Select the snapshot using distinct snapshot dates only.
CREATE TEMP TABLE inventory_snapshot_months AS
SELECT
m.billing_month,
MAX(d.snapshot_date) AS snapshot_date
FROM months AS m
JOIN (
SELECT DISTINCT _snapshot_date AS snapshot_date
FROM `PROJECT_ID.RAW_DATASET.raw_billed_inventory`
WHERE _snapshot_date <= LAST_DAY(end_month)
) AS d
ON d.snapshot_date <= m.month_end
GROUP BY m.billing_month;

-- Read rows belonging to the selected snapshot.
CREATE TEMP TABLE inventory_month_end AS
SELECT
m.billing_month,
i.INVENTORY_ID,
i.CUSTOMER_ID,
i.SERVICE_LOCATION_ID,
i.ACTIVATION_DT,
i.DEACTIVATION_DT
FROM inventory_snapshot_months AS m
JOIN `PROJECT_ID.RAW_DATASET.raw_billed_inventory` AS i
ON i._snapshot_date = m.snapshot_date
WHERE i._snapshot_date <= LAST_DAY(end_month);

-- Apply the same snapshot selection to geography.
CREATE TEMP TABLE geography_snapshot_months AS
SELECT
m.billing_month,
MAX(d.snapshot_date) AS snapshot_date
FROM months AS m
JOIN (
SELECT DISTINCT _snapshot_date AS snapshot_date
FROM `PROJECT_ID.RAW_DATASET.dim_geo_info`
WHERE _snapshot_date <= LAST_DAY(end_month)
) AS d
ON d.snapshot_date <= m.month_end
GROUP BY m.billing_month;

CREATE TEMP TABLE geography_month_end AS
SELECT
m.billing_month,
g.GEOGRAPHIC_ADDRESS_ID,
g.POSTAL_CODE
FROM geography_snapshot_months AS m
JOIN `PROJECT_ID.RAW_DATASET.dim_geo_info` AS g
ON g._snapshot_date = m.snapshot_date
WHERE g._snapshot_date <= LAST_DAY(end_month);

-- Service-location geography at month-end.
CREATE TEMP TABLE geography_monthly AS
SELECT
cm.customer_id,
cm.billing_month,
CASE
WHEN COUNT(DISTINCT g.POSTAL_CODE) > 1 THEN 'MULTIPLE'
WHEN COUNTIF(
i.SERVICE_LOCATION_ID IS NOT NULL
AND g.POSTAL_CODE IS NULL
) > 0 THEN NULL
ELSE MIN(g.POSTAL_CODE)
END AS service_postal_code,
CASE
WHEN COUNT(DISTINCT i.SERVICE_LOCATION_ID) = 0 THEN NULL
ELSE COUNT(DISTINCT i.SERVICE_LOCATION_ID) > 1
END AS has_multiple_service_locations
FROM customer_months AS cm
LEFT JOIN inventory_month_end AS i
ON cm.customer_id = i.CUSTOMER_ID
AND cm.billing_month = i.billing_month
AND DATE(i.ACTIVATION_DT) <= cm.month_end
AND (
i.DEACTIVATION_DT IS NULL
OR DATE(i.DEACTIVATION_DT) > cm.month_end
)
LEFT JOIN geography_month_end AS g
ON i.SERVICE_LOCATION_ID = g.GEOGRAPHIC_ADDRESS_ID
AND cm.billing_month = g.billing_month
GROUP BY cm.customer_id, cm.billing_month;

-- Number of valid contracts at each month-end.
-- Prerequisite: contract versions retained with _ingested_at.
-- Prepare availability intervals for contract versions.
-- The preceding checks block conflicting versions.
CREATE TEMP TABLE contract_version_intervals AS
WITH distinct_versions AS (
SELECT DISTINCT
CUSTOMER_ID AS customer_id,
CONTRACT_ID AS contract_id,
DATE(EFFECTIVE_START_DT) AS effective_start_date,
DATE(EFFECTIVE_END_DT) AS effective_end_date,
_ingested_at AS known_from
FROM `PROJECT_ID.RAW_DATASET.raw_product_subscriptions`
WHERE _ingested_at
< TIMESTAMP(DATE_ADD(end_month, INTERVAL 1 MONTH))
)
SELECT
*,
LEAD(known_from) OVER (
PARTITION BY customer_id, contract_id
ORDER BY known_from
) AS known_until
FROM distinct_versions;

-- Version of each contract known at month-end.
-- Retain customers without an available contract.
CREATE TEMP TABLE contract_versions_month_end AS
SELECT
cm.customer_id,
cm.billing_month,
cm.month_end,
s.contract_id,
s.effective_start_date,
s.effective_end_date
FROM customer_months AS cm
LEFT JOIN contract_version_intervals AS s
ON cm.customer_id = s.customer_id
AND DATE(s.known_from) <= cm.month_end
AND (
s.known_until IS NULL
OR DATE(s.known_until) > cm.month_end
);

CREATE TEMP TABLE contracts_monthly AS
SELECT
customer_id,
billing_month,
COUNT(DISTINCT IF(
effective_start_date <= month_end
AND effective_end_date >= month_end,
contract_id,
NULL
)) AS active_contract_count
FROM contract_versions_month_end
GROUP BY customer_id, billing_month;

-- Use the same historically selected sign-up date as the customer-month spine.
CREATE TEMP TABLE tenure_monthly AS
SELECT
customer_id,
billing_month,
DATE_DIFF(month_end, customer_since_date, DAY) AS customer_tenure_days
FROM customer_months;

-- Assemble features before validation and publication.
CREATE TEMP TABLE final_candidate AS
SELECT
cm.customer_id,
cm.billing_month,
t.customer_tenure_days,
p.customer_type,
p.credit_score,
u.monthly_data_usage_gb,
c.active_contract_count,
pay.has_late_payment_in_month,
pay.has_overdue_invoice_at_month_end,
sc.billing_ticket_count,
sc.technical_ticket_count,
sq.not_resolved_first_contact_count,
sq.average_satisfaction_score,
sq.satisfaction_rating_count,
fr.failed_request_count,
g.service_postal_code,
g.has_multiple_service_locations
FROM customer_months AS cm
LEFT JOIN tenure_monthly AS t
USING (customer_id, billing_month)
LEFT JOIN profile_monthly AS p
USING (customer_id, billing_month)
LEFT JOIN usage_monthly AS u
USING (customer_id, billing_month)
LEFT JOIN contracts_monthly AS c
USING (customer_id, billing_month)
LEFT JOIN payments_monthly AS pay
USING (customer_id, billing_month)
LEFT JOIN support_counts_monthly AS sc
USING (customer_id, billing_month)
LEFT JOIN support_quality_monthly AS sq
USING (customer_id, billing_month)
LEFT JOIN failed_requests_monthly AS fr
USING (customer_id, billing_month)
LEFT JOIN geography_monthly AS g
USING (customer_id, billing_month);



-- FINAL_CANDIDATE_CHECKS

-- Keys must be populated.
ASSERT (
SELECT COUNT(*) = 0
FROM final_candidate
WHERE customer_id IS NULL OR billing_month IS NULL
) AS 'Missing customer identifier or month';

-- Exactly one row per customer and month.
ASSERT (
SELECT COUNT(*) = 0
FROM (
SELECT customer_id, billing_month
FROM final_candidate
GROUP BY customer_id, billing_month
HAVING COUNT(*) > 1
)
) AS 'Duplicate customer-month rows in the result';

-- Assembly must preserve the original row count.
ASSERT (
(SELECT COUNT(*) FROM final_candidate)
=
(SELECT COUNT(*) FROM customer_months)
) AS 'Row count changed during joins';

-- All months must fall within the requested period.
ASSERT (
SELECT COUNT(*) = 0
FROM final_candidate
WHERE billing_month < start_month
OR billing_month > end_month
) AS 'Month outside the requested period';

-- Known customer tenure must not be negative.
ASSERT (
SELECT COUNT(*) = 0
FROM final_candidate
WHERE customer_tenure_days < 0
) AS 'Negative customer tenure';


-- MART_PUBLICATION
-- Prerequisite: the PROJECT_ID.MART_DATASET dataset exists.
-- Airflow must prevent concurrent publications over overlapping month ranges.

CREATE TABLE IF NOT EXISTS
`PROJECT_ID.MART_DATASET.mart_churn_features_flat`
PARTITION BY billing_month
CLUSTER BY customer_id
AS
SELECT *
FROM final_candidate
WHERE FALSE;

BEGIN TRANSACTION;

DELETE FROM `PROJECT_ID.MART_DATASET.mart_churn_features_flat`
WHERE billing_month BETWEEN start_month AND end_month;

INSERT INTO `PROJECT_ID.MART_DATASET.mart_churn_features_flat` (
customer_id,
billing_month,
customer_tenure_days,
customer_type,
credit_score,
monthly_data_usage_gb,
active_contract_count,
has_late_payment_in_month,
has_overdue_invoice_at_month_end,
billing_ticket_count,
technical_ticket_count,
not_resolved_first_contact_count,
average_satisfaction_score,
satisfaction_rating_count,
failed_request_count,
service_postal_code,
has_multiple_service_locations
)
SELECT
customer_id,
billing_month,
customer_tenure_days,
customer_type,
credit_score,
monthly_data_usage_gb,
active_contract_count,
has_late_payment_in_month,
has_overdue_invoice_at_month_end,
billing_ticket_count,
technical_ticket_count,
not_resolved_first_contact_count,
average_satisfaction_score,
satisfaction_rating_count,
failed_request_count,
service_postal_code,
has_multiple_service_locations
FROM final_candidate;

COMMIT TRANSACTION;
