# Horizon IRC External Reads & Writes Demo

Query Snowflake-managed Apache Iceberg tables from Apache Spark through the
**Snowflake Horizon Catalog** (Iceberg REST Catalog API). This demo runs
entirely within Snowflake using Notebook Container Runtime -- no external
infrastructure required.

## Architecture

```text
┌──────────────────────────────────────────────────────────────────┐
│  Snowflake Notebook Container Runtime                            │
│  ┌────────────────────────────────────┐                          │
│  │  PySpark (local[*])                │                          │
│  │  + Iceberg REST Catalog Client     │                          │
│  └──────────┬─────────────────────────┘                          │
│             │ HTTPS (PAT or Key-Pair)                            │
│             ▼                                                    │
│  ┌──────────────────────────────────────┐                        │
│  │  Horizon IRC REST API               │                        │
│  │  /polaris/api/catalog               │                        │
│  │  (RBAC + Masking + Row Access)      │                        │
│  └──────────┬───────────────────────────┘                        │
│             │ Vended Credentials                                 │
│             ▼                                                    │
│  ┌──────────────────────────────────────┐                        │
│  │  S3 External Volume                 │                        │
│  │  (Iceberg data + metadata files)    │                        │
│  └──────────────────────────────────────┘                        │
└──────────────────────────────────────────────────────────────────┘
```

## What This Demo Shows

| Feature | How It's Demonstrated |
|---|---|
| **External Reads** | Spark reads Iceberg tables via Horizon REST API |
| **External Writes** | Spark inserts rows back into Iceberg tables |
| **RBAC** | Two roles with different schema/table access |
| **Column Masking** | PII fields (email, phone, SSN) masked per role |
| **Row Access Policy** | Analysts see only US-WEST region orders |
| **PAT Auth** | Programmatic Access Tokens for service users |
| **Key-Pair Auth** | JWT-based authentication (production pattern) |

## Prerequisites

- Snowflake account with Iceberg support and Horizon IRC enabled
- An existing S3 external volume configured in Snowflake
- ACCOUNTADMIN access to run the setup script
- (Optional) Conda for local development fallback

## Quickstart

### Option A: Snowflake Notebook Container Runtime (Recommended)

1. **Run the admin setup** -- Open `notebooks/01_admin_setup.sql` in a Snowflake
   SQL Worksheet and execute as ACCOUNTADMIN. Copy the generated PAT tokens.

2. **Upload the demo notebook** -- Upload `notebooks/02_horizon_irc_spark_demo.ipynb`
   to a Snowflake stage:
   ```sql
   PUT file:///path/to/02_horizon_irc_spark_demo.ipynb @MY_STAGE AUTO_COMPRESS=FALSE;
   ```

3. **Create the notebook** -- In Snowsight, create a new Notebook from the stage
   file, select the `SPARK_DEMO_POOL` compute pool with Container Runtime.

4. **Configure and run** -- Update the configuration cell with your account
   identifier and PAT tokens, then run all cells.

### Option B: Local Jupyter Notebook

1. Run `01_admin_setup.sql` in Snowflake as above.
2. Install the conda environment:
   ```bash
   conda env create -f environment.yml
   conda activate iceberg-horizon-demo
   jupyter notebook
   ```
3. Open `notebooks/02_horizon_irc_spark_demo.ipynb` and run.

## Project Structure

```
├── README.md                                   # This file
├── .gitignore
├── environment.yml                             # Conda env (local fallback)
├── notebooks/
│   ├── 01_admin_setup.sql                      # Snowflake SQL setup
│   └── 02_horizon_irc_spark_demo.ipynb         # PySpark demo notebook
└── .cursor/rules/
    └── deploy-to-github.mdc                    # Cursor deploy rule
```

## Demo Flow / Talking Points

1. **Setup** (01_admin_setup.sql) -- Show the RBAC matrix, explain why two personas
2. **Engineer connects** -- PAT auth, sees all schemas and tables
3. **Read all tables** -- Point out full access, data integrity
4. **Masking demo** -- Query USER_PROFILES, show which fields are masked and why
5. **Write demo** -- Insert from Spark, verify in Snowflake
6. **Analyst connects** -- Switch PAT, same notebook, different view
7. **Row access policy** -- Same query on CUSTOMER_ORDERS, fewer rows
8. **Denied access** -- Analyst tries ANALYTICS/RESTRICTED, gets blocked
9. **Summary** -- Snowflake governance extends to external engines seamlessly

## References

- [Snowflake Horizon IRC Documentation](https://docs.snowflake.com/en/user-guide/tables-iceberg-query-using-external-query-engine-snowflake-horizon)
- [Tutorial: Create Your First Iceberg Table](https://docs.snowflake.com/en/user-guide/tutorials/create-your-first-iceberg-table)
- [Enforce Data Protection Policies on Iceberg Tables](https://docs.snowflake.com/en/user-guide/tables-iceberg-query-using-external-query-engine-snowflake-horizon-enforce-data-protection-policies)

## Troubleshooting

| Issue | Fix |
|---|---|
| `NotAuthorized` on all tables | Verify PAT is valid and role has USAGE on external volume |
| Underscore in account URL fails | Replace `_` with `-` in the account identifier URL |
| External write 403 | Enable account-level write flags (step 8 in setup SQL) |
| `MONITOR` grant error | Ensure MONITOR is granted on schemas (required for SHOW TABLES) |
| Spark can't resolve packages | Ensure the container/machine has internet access for Maven |
