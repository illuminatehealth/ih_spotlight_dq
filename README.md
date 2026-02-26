# IH Spotlight DQ dbt Package

A dbt package for Spotlight data quality monitoring that combines:

- Month-level reconciliation metrics
- Input-layer completeness checks (non-terminology)
- Terminology validity checks
- Optional history persistence for official runs, with latest/previous run views

All outputs are PHI-safe aggregate summaries.

## What this package builds

### Reconciliation models

- `reconciliation__monthly_summary`
  - Grain: `data_source`, `payer`, `plan`, `year_month_int` (`YYYYMM`)
  - Measures:
    - `member_months`, `members`
    - `claims`, `claim_lines`, `paid_amount`
    - `members_with_claims`, `pct_members_with_claims`
    - `claims_per_1000`, `pmpm_paid`, `avg_paid_per_claim`

- `reconciliation_history__monthly_summary`
  - Incremental history table persisted only on official runs (`dq_persist_results: true`)

- `reconciliation_history__monthly_summary__latest_official_run`
- `reconciliation_history__monthly_summary__previous_official_run`
  - Convenience views for latest and previous persisted runs

### DQ current-run models

- `dq_results__input_layer_field_summary`
  - Non-terminology checks (null/non-null/not_applicable) at the grain:
    `run_id`, `data_source`, `payer`, `plan`, `model_name`, `field_name`

- `dq_results__terminology_field_summary`
  - Terminology checks (valid/invalid/null/not_applicable) at the grain:
    `run_id`, `data_source`, `payer`, `plan`, `model_name`, `field_name`

### Fields evaluated by DQ checks

- Eligibility fields:
  - `race`
  - `dual_status_code`
- Medical claim fields:
  - `drg_code` (validated using `drg_code_type` against MS-DRG or APR-DRG sets)
  - `revenue_center_code`
  - `hcpcs_code`
  - `diagnosis_code_1`
  - `diagnosis_code_2`
  - `diagnosis_code_3`
  - `procedure_code_1`
  - `procedure_code_2`
  - `procedure_code_3`
  - `discharge_disposition_code`
  - `admit_source_code`
  - `admit_type_code`
  - `bill_type_code`
  - `place_of_service_code`
  - `rendering_npi`
  - `billing_npi`
  - `facility_npi`
- Pharmacy claim fields:
  - `ndc_code`

### DQ history models

- `dq_history__input_layer_field_summary`
- `dq_history__terminology_field_summary`
  - Incremental history tables persisted only on official runs (`dq_persist_results: true`)

- `dq_history__input_layer_field_summary__latest_official_run`
- `dq_history__input_layer_field_summary__previous_official_run`
- `dq_history__terminology_field_summary__latest_official_run`
- `dq_history__terminology_field_summary__previous_official_run`
  - Convenience views for latest and previous persisted runs

## Required upstream models

This package expects Tuva-style upstream refs to already exist in the downstream project:

- Core:
  - `core__medical_claim`
  - `core__member_months`
- Input layer:
  - `input_layer__eligibility`
  - `input_layer__medical_claim`
  - `input_layer__pharmacy_claim`
- Reference:
  - `reference_data__calendar`
- Terminology (used by `dq_results__terminology_field_summary`):
  - `terminology__race`
  - `terminology__medicare_dual_eligibility`
  - `terminology__ms_drg`
  - `terminology__apr_drg`
  - `terminology__revenue_center`
  - `terminology__hcpcs_level_2`
  - `terminology__icd_10_cm`
  - `terminology__icd_10_pcs`
  - `terminology__discharge_disposition`
  - `terminology__admit_source`
  - `terminology__admit_type`
  - `terminology__bill_type`
  - `terminology__place_of_service`
  - `terminology__provider`
  - `terminology__ndc`

## Install in a downstream dbt project

Add this package to your downstream `packages.yml`:

```yml
packages:
  - git: "https://github.com/<your-org>/ih_spotlight_dq.git"
    revision: "main"
```

This package does not pin transitive dependencies. The downstream project must include compatible versions of:

- Tuva package (or equivalent fork that provides the refs above)
- `dbt_utils`

Example:

```yml
packages:
  - package: tuva-health/the_tuva_project
    version: [">=0.15.0", "<1.0.0"]
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

Then run:

```bash
dbt deps
```

## Runtime variables

- `dq_persist_results` (default: `false`)
  - When `true`, history models persist rows; when `false`, history models emit zero rows
- `dq_run_id` (default: dbt `invocation_id`)
  - Run identifier stamped onto outputs
- `tuva_last_run` (optional)
  - Run timestamp used by metadata logic; falls back to `run_started_at`

## How to run

### Build current-run summaries only

```bash
dbt run -s reconciliation__monthly_summary dq_results__input_layer_field_summary dq_results__terminology_field_summary
```

### Build all Spotlight models and persist official history

```bash
dbt run -s tag:spotlight --vars '{dq_persist_results: true}'
```

Optional explicit run metadata:

```bash
dbt run -s tag:spotlight --vars '{dq_persist_results: true, dq_run_id: "official_2026_02_25", tuva_last_run: "2026-02-25 00:00:00"}'
```

## Data tests

Run tests for all models in this package:

```bash
dbt test -s tag:spotlight
```

Included tests cover:

- Unique run/grain combinations on summary and history tables
- Percentage bounds (0-100) for DQ percent metrics

## Latest vs previous run analysis

Use [analyses/dq_terminology_latest_vs_previous_deltas.sql](/Users/tom/Documents/ih/ih_spotlight_dq/analyses/dq_terminology_latest_vs_previous_deltas.sql) to compare terminology DQ deltas between:

- `dq_history__terminology_field_summary__latest_official_run`
- `dq_history__terminology_field_summary__previous_official_run`

## Output schema behavior

This package does not override `generate_schema_name`. Final relation schema naming is controlled by the downstream project's dbt schema configuration and macro behavior.
