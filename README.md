# Meridian Stays: End-to-End Lakehouse Architecture & Governance Implementation

**Project Code**: `meridian_t02`  
**Workspace**: Databricks Free Edition on AWS / Unity Catalog  
**Runtime**: Lakeflow Spark Declarative Pipelines, Serverless SQL Warehouses  
**Orchestration & IaC**: Databricks Asset Bundles (DABs), Lakeflow Jobs  

---

## 1. Executive Summary & Architecture

Meridian Stays is an enterprise-grade lakehouse implementation designed to process, govern, and analyze high-volume hospitality operations, guest clickstream data, reservation transactions, and property reviews. 

The architecture transitions data across a formal Medallion design pattern:
1. **Landing Tier**: External cloud landing volume (`/Volumes/meridian_t02/landing/raw_files`) receiving multi-entity simulated batch drops.
2. **Bronze Tier**: Append-only Delta streaming tables ingested via Auto Loader (`cloudFiles`) with automated schema inference and bad-record isolation.
3. **Silver Tier**: Cleaned, typed, and deduplicated tables featuring SCD Type 2 CDC event processing, JSON event unnesting, and stateful 30-minute clickstream sessionization.
4. **Gold Tier (Star Schema)**: Star schema optimized for BI consumption utilizing Point-in-Time (PIT) dimensional joins, metric views (`mv_stays_kpis`), dynamic Row-Level Security (RLS), and Column-Level Security (CLS).
5. **Consumption**: AI/BI Executive Dashboards, System Observability Dashboards, Databricks Genie NL Agent, and Power BI DirectQuery reporting.

Landing Volume ---> Bronze (Auto Loader) ---> Silver (CDC SCD2 & Sessionization)
|
v
Power BI (DirectQuery) <--- Metric Views <--- Gold (Star Schema + PIT Joins)
^                                                |
|                                                +---> Dynamic RLS / CLS Masks
Databricks Genie NL Agent                                   +---> Automated Quality Gates


---

## 2. Medallion Layer Specifications

### Bronze Layer (Ingestion)
- Ingested via Lakeflow Declarative Pipelines utilizing Auto Loader (`cloudFiles`).
- Format: Native Delta Lake with schema rescue column enabled (`_rescued_data`).
- Tables: `bronze.raw_users`, `bronze.raw_properties`, `bronze.raw_bookings`, `bronze.raw_reviews`, `bronze.raw_clickstream`.

### Silver Layer (Cleansing, CDC & Sessionization)
- **CDC Processing**: Implements Slowly Changing Dimensions Type 2 (SCD2) on `dim_users` and `dim_properties` to track historical state changes (`is_current`, `valid_from`, `valid_to`).
- **Sessionization**: Custom stateful session windowing grouping consecutive clickstream interactions with an inactivity timeout threshold of 30 minutes, calculating bounce indicators and funnel step flags.
- **Contract Enforcement & Quarantine**: Data quality rules quarantine malformed records (e.g., negative booking amounts, out-of-range review scores) into dedicated `ops.quarantine_*` tables without interrupting pipeline execution.

### Gold Layer (Dimensional Serving)
- **Dimensional Model**:
  - `gold.dim_users` (SCD2 Conformed Dimension)
  - `gold.dim_properties` (SCD2 Conformed Dimension)
  - `gold.fct_sessions` (Granularity: 1 row per user session)
  - `gold.fct_bookings` (Granularity: 1 row per confirmed/cancelled booking)
  - `gold.fct_reviews` (Granularity: 1 row per moderation-approved review)
- **Point-in-Time (PIT) Joins**: Fact tables are joined to dimension records where `fact.event_timestamp BETWEEN dim.valid_from AND COALESCE(dim.valid_to, '9999-12-31')`, ensuring zero historical attribution leakage.
- **Semantic Metric Views**: `gold.mv_stays_kpis` and `gold.mv_reviews_kpis` encapsulate core business metrics using Databricks `MEASURE(...)` syntax, reconciling with 100% mathematical parity against raw transactional queries.

---

## 3. Governance, Security, and Compliance Matrix

Governance is enforced natively at the Unity Catalog catalog level via dynamic UDFs and an external entitlement mapping table.

### Security Matrix
| Principal / Group | Row-Level Access (`user_country`) | Sensitive Data Access (`email`, `review_comment`) | Operational Scope |
| :--- | :--- | :--- | :--- |
| `t02_admins` | All countries (unfiltered) | Masked unless also in `t02_pii_readers` | Full administrative & schema management |
| `t02_pii_readers` | Governed by entitlement table | **Unmasked** clear-text PII | Compliance & elevated dispute resolution |
| General Analysts | Restrictive (`gov.entitlements` mapped) | **Masked** (`ab****@domain`, `[REDACTED]`) | Read-only analytics on `gold` schema |


erDiagram
    %% CONFORMED DIMENSIONS (SCD Type 2)
    dim_users {
        string user_id PK
        string user_name
        string email "Masked (CLS)"
        string user_country "Filtered (RLS)"
        string loyalty_tier
        timestamp valid_from "SCD2"
        timestamp valid_to "SCD2"
        boolean is_current
    }

    dim_properties {
        string property_id PK
        string title
        string property_type
        string country
        timestamp valid_from "SCD2"
        timestamp valid_to "SCD2"
        boolean is_current
    }

    %% FACT TABLES
    fct_bookings {
        string booking_id PK
        string user_id FK
        string property_id FK
        string booking_status
        decimal total_amount
        int guests_count
        date check_in
        date check_out
        timestamp event_timestamp
    }

    fct_sessions {
        string session_id PK
        string user_id FK
        string device_type
        int has_search
        int has_view
        int has_booking_attempt
        int is_booked
        timestamp session_start
        timestamp session_end
    }

    fct_reviews {
        string review_id PK
        string user_id FK
        string property_id FK
        int rating
        string review_comment "Masked (CLS)"
        string moderation_status
        timestamp review_date
    }

    %% RELATIONSHIPS (Point-in-Time Joins)
    dim_users ||--o{ fct_bookings : "makes (PIT Join)"
    dim_properties ||--o{ fct_bookings : "receives (PIT Join)"
    
    dim_users ||--o{ fct_sessions : "browses (PIT Join)"
    
    dim_users ||--o{ fct_reviews : "writes (PIT Join)"
    dim_properties ||--o{ fct_reviews : "receives (PIT Join)"

    %% SEMANTIC METRIC VIEWS (Aggregations)
    mv_stays_kpis {
        date session_date
        string user_country
        string device_type
        string user_type
        measure active_users
        measure total_sessions
        measure search_to_booking_conversion
    }
    
    mv_reviews_kpis {
        date review_date
        string user_country
        measure total_reviews
        measure average_rating
        measure deleted_review_share
    }

    %% MACRO RELATIONSHIPS (Rollups)
    fct_sessions }o--|| mv_stays_kpis : "Aggregates into"
    fct_bookings }o--|| mv_stays_kpis : "Aggregates into"
    fct_reviews }o--|| mv_reviews_kpis : "Aggregates into"



### Key Governance Implementations
1. **Dynamic Row-Level Security (RLS)**:
   - Filter UDF: `gov.fn_country_row_filter(country_val)`
   - Dynamically resolves caller membership in `t02_admins` or queries `meridian_t02.gov.entitlements` to authorize access per country.
2. **Column-Level Security (CLS)**:
   - Partial Email Mask: `gov.fn_mask_email(email_val)` exposes the first two characters and domain, redacting user handles with asterisks (`ab****@example.com`).
   - Text Redaction: `gov.fn_mask_comment(comment_val)` replaces free-text review entries with `[REDACTED]` for unauthorized personas.
3. **Materialized View Protection**:
   - Security policies are attached using `ALTER MATERIALIZED VIEW ... SET ROW FILTER` and `SET MASK`, decoupling ingestion-time write locks from query-time authorization enforcement.

---

## 4. Orchestration, Quality Gates, and CI/CD

The entire environment is configured as code via **Databricks Asset Bundles (DABs)** (`databricks.yml`) and orchestrated using **Lakeflow Jobs**.

### Job DAG Architecture
1. **`land_batch_files`** (`notebook_task`): Simulates ingestion drops into the Unity Catalog volume.
2. **`execute_medallion_pipeline`** (`pipeline_task`): Triggers the declarative Bronze-to-Gold pipeline.
3. **`verify_metric_views`** (`notebook_task`): Validates semantic metric view integrity.
4. **`evaluate_quality_gate`** (`notebook_task`): Runs automated assertions against Gold tables, logging outcomes to `ops.data_quality_results`.

### Automated Quality Gate & Failure Recovery
- Asserts primary key uniqueness, referential foreign key integrity, non-null constraints, and positive booking values.
- Throws an explicit execution exception upon threshold violation, halting downstream actions.
- Supports fine-grained **Task Repair**: In the event of an upstream or quality failure, engineers can repair the single failed node without re-running long-running extraction stages.

---

## 5. Consumption & Analytics

- **AI/BI Business KPI Dashboard**: Published dashboard visualizing session drop-off funnels, conversion rates by device and geography, and revenue trends.
- **System Observability Dashboard**: Tracks query latencies via `system.query.history`, quarantine error distributions, and automated quality gate audit logs.
- **Databricks Genie Agent**: Self-service natural language conversational interface configured with semantic business context and table guidelines.
- **Power BI DirectQuery**: Direct connection to Unity Catalog SQL Warehouses with zero data import, preserving real-time row-filtering and column masking at the report layer.

---

## 6. Key Architectural Tradeoffs

1. **Materialized Views vs Delta Tables for Gold**:
   - *Decision*: Pipeline Gold tables are registered as `MATERIALIZED VIEW` objects within Unity Catalog Direct Publishing Mode (DPM).
   - *Tradeoff*: Requires `ALTER MATERIALIZED VIEW` syntax for governance DDL; ensures ACID consistency and automatic incremental refresh handling across dependencies.
2. **Dynamic Entitlement Table vs Static UDF Case Statements**:
   - *Decision*: Country access boundaries are managed via `gov.entitlements`.
   - *Tradeoff*: Adds an evaluation subquery during access checks, but eliminates hardcoded security logic and allows real-time role updates without code deployments.
3. **DirectQuery vs Import Mode in Power BI**:
   - *Decision*: Enforced DirectQuery mode.
   - *Tradeoff*: Minor network latency during interactive dashboard cross-filtering, but strictly prevents PII data exfiltration into external `.pbix` desktop caches and enforces catalog-level RLS.
