# Customer Churn Feature Mart | GCP Data Engineering

**Data Engineering portfolio project — BigQuery SQL, historical data modeling, data quality, and cloud architecture.**

## From business need to data product

**Business objective: equip Data Science teams with reliable customer information to investigate and predict churn, helping a telecommunications business identify customers who may be at risk of leaving.**

Telecommunications companies hold customer information across separate operational systems: profiles, subscriptions, usage, invoices, support interactions, service requests, inventory, and geography. These sources arrive at different frequencies and have different grains and historical semantics.

**The Data Engineering challenge is to convert disconnected operational records into comparable, historically meaningful customer features** that a Data Science team can use for churn analysis and predictive modeling.

## Turning business questions into data requirements

I translated this business need into a BigQuery feature mart, **`mart_churn_features_flat`**, with **one row per customer per calendar month**.

| Business question | Data Engineering response |
| --- | --- |
| Are technical issues associated with churn? | Support-ticket counts, first-contact resolution measures, and customer satisfaction features. |
| Could billing friction contribute to churn risk? | Late-payment and overdue-invoice indicators. |
| How are services used and subscriptions maintained? | Monthly data usage and active-contract counts. |
| What was known about the customer at the observation date? | Historical versions selected using availability timestamps and dated snapshots. |
| How do we deliver reliable customer-month comparisons? | A consistent grain, aggregation before joins, quality checks, and repeatable publication. |

These features **support investigation and predictive modeling**; the project does not claim that any particular feature causes churn.

## My Data Engineering contribution

I developed the **BigQuery Standard SQL** transformation to reconstruct historical customer states, aggregate operational activity to a common monthly grain, and assemble an analysis-ready customer feature table.

The SQL includes source-readiness checks, version-conflict checks, customer-month uniqueness and row-count validations, date-bound checks, and targeted historical recalculation. The final mart is partitioned by `billing_month`, clustered by `customer_id`, and published using a transactional `DELETE` + `INSERT` for selected months.

I also designed the **GCP data architecture**, covering source-specific ingestion patterns (CDC, append, and snapshots), Cloud Storage retention, BigQuery raw and transformation layers, and Airflow orchestration responsibilities.

## What this demonstrates to an employer

**I can start with a business problem, identify relevant source data, define the analytical grain, engineer historically consistent features, and validate a data product against the needs of its consumers.**

This project demonstrates the ability to translate a business requirement into a dependable Data Engineering solution that makes operational data usable by Data Science teams.

## Repository contents

- `sql/mart_churn_features_flat.sql` — BigQuery transformation and checks
- `architecture/gcp_customer_churn_architecture.pdf` — cloud architecture diagram
- `docs/historical_changes_and_resilience.pdf` — temporal design and recovery decisions
- `docs/source_data_specifications.xlsx` — source specifications

## Execution context

The SQL expects prepared BigQuery raw tables and ingestion metadata such as `_ingested_at` and `_snapshot_date`. Configure project/dataset identifiers and provide `start_month`, `end_month`, and `sources_ready` query parameters.

The repository documents the developed transformation and the accompanying architecture design; it does not claim a deployed churn model or measured churn reduction.
