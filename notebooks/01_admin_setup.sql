-- =============================================================================
-- Horizon IRC External Reads/Writes Demo -- Snowflake Admin Setup
-- =============================================================================
-- Run this entire script in a Snowflake SQL Worksheet as ACCOUNTADMIN.
-- It creates the database, schemas, Iceberg tables, roles, users, policies,
-- and compute pool needed for the PySpark demo notebook.
--
-- Prerequisites:
--   1. An existing S3 external volume (replace <YOUR_EXTERNAL_VOLUME> below)
--   2. An existing warehouse for running DDL (replace <YOUR_WAREHOUSE>)
--   3. ACCOUNTADMIN role
--
-- Reference: https://docs.snowflake.com/en/user-guide/tables-iceberg-query-using-external-query-engine-snowflake-horizon
-- =============================================================================

-- ─────────────────────────────────────────────
-- 0. VARIABLES — UPDATE THESE FOR YOUR ACCOUNT
-- ─────────────────────────────────────────────
SET EXT_VOLUME    = 'SF_EXTERNAL_VOLUME';        -- your S3 external volume name
SET WAREHOUSE     = 'COMPUTE_WH';                -- any active warehouse
SET DB            = 'ICEBERG_DEMO_DB';

-- ─────────────────────────────────────────────
-- 1. ROLES & SERVICE USERS
-- ─────────────────────────────────────────────
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE IDENTIFIER($WAREHOUSE);

CREATE ROLE IF NOT EXISTS DATA_ENGINEER;
CREATE ROLE IF NOT EXISTS DATA_ANALYST;

GRANT ROLE DATA_ENGINEER TO ROLE ACCOUNTADMIN;
GRANT ROLE DATA_ANALYST  TO ROLE ACCOUNTADMIN;

CREATE USER IF NOT EXISTS IRC_CLIENT_ENGINEER
  LOGIN_NAME = 'engineer_pat'
  TYPE = 'service';

CREATE USER IF NOT EXISTS IRC_CLIENT_ANALYST
  LOGIN_NAME = 'analyst_pat'
  TYPE = 'service';

GRANT ROLE DATA_ENGINEER TO USER IRC_CLIENT_ENGINEER;
GRANT ROLE DATA_ANALYST  TO USER IRC_CLIENT_ANALYST;

ALTER USER IRC_CLIENT_ENGINEER SET DEFAULT_ROLE = DATA_ENGINEER;
ALTER USER IRC_CLIENT_ANALYST  SET DEFAULT_ROLE = DATA_ANALYST;

-- External volume access (required for Horizon IRC reads)
GRANT USAGE ON EXTERNAL VOLUME IDENTIFIER($EXT_VOLUME) TO ROLE DATA_ENGINEER;
GRANT USAGE ON EXTERNAL VOLUME IDENTIFIER($EXT_VOLUME) TO ROLE DATA_ANALYST;

-- ─────────────────────────────────────────────
-- 2. DATABASE & SCHEMAS
-- ─────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS IDENTIFIER($DB);

-- Set external volume at database level (required for Horizon IRC)
ALTER DATABASE ICEBERG_DEMO_DB SET EXTERNAL_VOLUME = $EXT_VOLUME;

CREATE SCHEMA IF NOT EXISTS ICEBERG_DEMO_DB.SALES;
CREATE SCHEMA IF NOT EXISTS ICEBERG_DEMO_DB.ANALYTICS;
CREATE SCHEMA IF NOT EXISTS ICEBERG_DEMO_DB.RESTRICTED;

-- Engineer: full access to all schemas
GRANT USAGE ON DATABASE ICEBERG_DEMO_DB TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA ICEBERG_DEMO_DB.SALES      TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA ICEBERG_DEMO_DB.ANALYTICS   TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA ICEBERG_DEMO_DB.RESTRICTED  TO ROLE DATA_ENGINEER;
GRANT MONITOR ON DATABASE ICEBERG_DEMO_DB          TO ROLE DATA_ENGINEER;
GRANT MONITOR ON SCHEMA ICEBERG_DEMO_DB.SALES     TO ROLE DATA_ENGINEER;
GRANT MONITOR ON SCHEMA ICEBERG_DEMO_DB.ANALYTICS TO ROLE DATA_ENGINEER;
GRANT MONITOR ON SCHEMA ICEBERG_DEMO_DB.RESTRICTED TO ROLE DATA_ENGINEER;

GRANT CREATE ICEBERG TABLE ON SCHEMA ICEBERG_DEMO_DB.SALES      TO ROLE DATA_ENGINEER;
GRANT CREATE ICEBERG TABLE ON SCHEMA ICEBERG_DEMO_DB.ANALYTICS   TO ROLE DATA_ENGINEER;
GRANT CREATE ICEBERG TABLE ON SCHEMA ICEBERG_DEMO_DB.RESTRICTED  TO ROLE DATA_ENGINEER;

-- Analyst: SALES schema only
GRANT USAGE ON DATABASE ICEBERG_DEMO_DB TO ROLE DATA_ANALYST;
GRANT USAGE ON SCHEMA ICEBERG_DEMO_DB.SALES TO ROLE DATA_ANALYST;
GRANT MONITOR ON DATABASE ICEBERG_DEMO_DB    TO ROLE DATA_ANALYST;
GRANT MONITOR ON SCHEMA ICEBERG_DEMO_DB.SALES TO ROLE DATA_ANALYST;

-- ─────────────────────────────────────────────
-- 3. ICEBERG TABLES WITH REALISTIC DATA
-- ─────────────────────────────────────────────
USE ROLE DATA_ENGINEER;
USE SCHEMA ICEBERG_DEMO_DB.SALES;

-- 3a. CUSTOMER_ORDERS — both roles can read (row-access-policy applied later)
CREATE OR REPLACE ICEBERG TABLE ICEBERG_DEMO_DB.SALES.CUSTOMER_ORDERS (
    order_id     INT,
    customer_id  INT,
    product      STRING,
    amount       DECIMAL(10,2),
    order_date   DATE,
    region       STRING
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'SF_EXTERNAL_VOLUME'
  BASE_LOCATION = 'sales/customer_orders';

INSERT INTO ICEBERG_DEMO_DB.SALES.CUSTOMER_ORDERS VALUES
  (1001, 501, 'Laptop Pro 15',       1299.99, '2025-01-15', 'US-WEST'),
  (1002, 502, 'Wireless Mouse',        29.99, '2025-01-16', 'US-EAST'),
  (1003, 503, 'USB-C Hub',             59.99, '2025-01-17', 'US-WEST'),
  (1004, 504, '4K Monitor',           449.99, '2025-02-01', 'EU-WEST'),
  (1005, 505, 'Mechanical Keyboard',   149.99, '2025-02-03', 'US-WEST'),
  (1006, 506, 'Laptop Pro 15',       1299.99, '2025-02-10', 'APAC'),
  (1007, 501, 'Webcam HD',             79.99, '2025-03-01', 'US-EAST'),
  (1008, 507, 'Standing Desk',        599.99, '2025-03-05', 'EU-WEST'),
  (1009, 508, 'Noise-Cancel Headset', 249.99, '2025-03-12', 'US-WEST'),
  (1010, 509, 'Laptop Pro 15',       1299.99, '2025-03-20', 'APAC');

-- 3b. PRODUCT_CATALOG — both roles can read
CREATE OR REPLACE ICEBERG TABLE ICEBERG_DEMO_DB.SALES.PRODUCT_CATALOG (
    product_id   INT,
    name         STRING,
    category     STRING,
    price        DECIMAL(10,2)
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'SF_EXTERNAL_VOLUME'
  BASE_LOCATION = 'sales/product_catalog';

INSERT INTO ICEBERG_DEMO_DB.SALES.PRODUCT_CATALOG VALUES
  (1, 'Laptop Pro 15',       'Electronics', 1299.99),
  (2, 'Wireless Mouse',      'Accessories',   29.99),
  (3, 'USB-C Hub',           'Accessories',   59.99),
  (4, '4K Monitor',          'Electronics',  449.99),
  (5, 'Mechanical Keyboard', 'Accessories',  149.99),
  (6, 'Webcam HD',           'Accessories',   79.99),
  (7, 'Standing Desk',       'Furniture',    599.99),
  (8, 'Noise-Cancel Headset','Audio',        249.99);

-- 3c. USER_PROFILES — engineer only, has PII with masking policies
USE SCHEMA ICEBERG_DEMO_DB.ANALYTICS;

CREATE OR REPLACE ICEBERG TABLE ICEBERG_DEMO_DB.ANALYTICS.USER_PROFILES (
    user_id    INT,
    name       STRING,
    email      STRING,
    phone      STRING,
    ssn_last4  STRING,
    region     STRING
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'SF_EXTERNAL_VOLUME'
  BASE_LOCATION = 'analytics/user_profiles';

INSERT INTO ICEBERG_DEMO_DB.ANALYTICS.USER_PROFILES VALUES
  (501, 'Alice Johnson',  'alice.johnson@acme.com',  '415-555-0101', '1234', 'US-WEST'),
  (502, 'Bob Smith',      'bob.smith@acme.com',      '212-555-0102', '5678', 'US-EAST'),
  (503, 'Carol Williams', 'carol.w@acme.com',        '415-555-0103', '9012', 'US-WEST'),
  (504, 'David Brown',    'david.b@globex.com',      '44-20-7946-0104', '3456', 'EU-WEST'),
  (505, 'Eva Martinez',   'eva.m@acme.com',          '415-555-0105', '7890', 'US-WEST'),
  (506, 'Frank Lee',      'frank.lee@initech.com',   '81-3-1234-0106', '2345', 'APAC'),
  (507, 'Grace Kim',      'grace.kim@globex.com',    '44-20-7946-0107', '6789', 'EU-WEST'),
  (508, 'Hank Davis',     'hank.d@acme.com',         '415-555-0108', '0123', 'US-WEST'),
  (509, 'Ivy Chen',       'ivy.chen@initech.com',    '81-3-1234-0109', '4567', 'APAC');

-- 3d. REVENUE_SUMMARY — engineer only, financial data
USE SCHEMA ICEBERG_DEMO_DB.RESTRICTED;

CREATE OR REPLACE ICEBERG TABLE ICEBERG_DEMO_DB.RESTRICTED.REVENUE_SUMMARY (
    region    STRING,
    quarter   STRING,
    revenue   DECIMAL(12,2)
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'SF_EXTERNAL_VOLUME'
  BASE_LOCATION = 'restricted/revenue_summary';

INSERT INTO ICEBERG_DEMO_DB.RESTRICTED.REVENUE_SUMMARY VALUES
  ('US-WEST', 'Q1-2025', 152499.50),
  ('US-EAST', 'Q1-2025',  89230.00),
  ('EU-WEST', 'Q1-2025', 104998.75),
  ('APAC',    'Q1-2025',  67500.25),
  ('US-WEST', 'Q2-2025', 178320.00),
  ('US-EAST', 'Q2-2025',  95100.00),
  ('EU-WEST', 'Q2-2025', 112400.50),
  ('APAC',    'Q2-2025',  73800.00);

-- ─────────────────────────────────────────────
-- 4. DATA PROTECTION POLICIES
-- ─────────────────────────────────────────────
USE ROLE ACCOUNTADMIN;

-- 4a. MASKING POLICY: email — visible to DATA_ENGINEER, masked for everyone else
CREATE OR REPLACE MASKING POLICY ICEBERG_DEMO_DB.ANALYTICS.EMAIL_MASK
  AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'DATA_ENGINEER') THEN val
    ELSE REGEXP_REPLACE(val, '.+@', '****@')
  END;

ALTER ICEBERG TABLE ICEBERG_DEMO_DB.ANALYTICS.USER_PROFILES
  ALTER COLUMN email SET MASKING POLICY ICEBERG_DEMO_DB.ANALYTICS.EMAIL_MASK;

-- 4b. MASKING POLICY: phone — masked for all roles (only ACCOUNTADMIN sees real value)
CREATE OR REPLACE MASKING POLICY ICEBERG_DEMO_DB.ANALYTICS.PHONE_MASK
  AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN val
    ELSE CONCAT(LEFT(val, 3), '-***-****')
  END;

ALTER ICEBERG TABLE ICEBERG_DEMO_DB.ANALYTICS.USER_PROFILES
  ALTER COLUMN phone SET MASKING POLICY ICEBERG_DEMO_DB.ANALYTICS.PHONE_MASK;

-- 4c. MASKING POLICY: ssn_last4 — always masked (even for DATA_ENGINEER)
CREATE OR REPLACE MASKING POLICY ICEBERG_DEMO_DB.ANALYTICS.SSN_MASK
  AS (val STRING) RETURNS STRING ->
  '****';

ALTER ICEBERG TABLE ICEBERG_DEMO_DB.ANALYTICS.USER_PROFILES
  ALTER COLUMN ssn_last4 SET MASKING POLICY ICEBERG_DEMO_DB.ANALYTICS.SSN_MASK;

-- 4d. ROW ACCESS POLICY: analysts see only US-WEST orders, engineers see all
CREATE OR REPLACE ROW ACCESS POLICY ICEBERG_DEMO_DB.SALES.REGION_FILTER
  AS (region_val STRING) RETURNS BOOLEAN ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'DATA_ENGINEER') THEN TRUE
    WHEN CURRENT_ROLE() = 'DATA_ANALYST' AND region_val = 'US-WEST' THEN TRUE
    ELSE FALSE
  END;

ALTER ICEBERG TABLE ICEBERG_DEMO_DB.SALES.CUSTOMER_ORDERS
  ADD ROW ACCESS POLICY ICEBERG_DEMO_DB.SALES.REGION_FILTER ON (region);

-- ─────────────────────────────────────────────
-- 5. GRANTS (FULLY QUALIFIED)
-- ─────────────────────────────────────────────

-- Engineer: SELECT on all tables
GRANT SELECT ON TABLE ICEBERG_DEMO_DB.SALES.CUSTOMER_ORDERS      TO ROLE DATA_ENGINEER;
GRANT SELECT ON TABLE ICEBERG_DEMO_DB.SALES.PRODUCT_CATALOG      TO ROLE DATA_ENGINEER;
GRANT SELECT ON TABLE ICEBERG_DEMO_DB.ANALYTICS.USER_PROFILES    TO ROLE DATA_ENGINEER;
GRANT SELECT ON TABLE ICEBERG_DEMO_DB.RESTRICTED.REVENUE_SUMMARY TO ROLE DATA_ENGINEER;

-- Engineer: INSERT for write demo
GRANT INSERT ON TABLE ICEBERG_DEMO_DB.SALES.CUSTOMER_ORDERS TO ROLE DATA_ENGINEER;
GRANT INSERT ON TABLE ICEBERG_DEMO_DB.SALES.PRODUCT_CATALOG TO ROLE DATA_ENGINEER;

-- Analyst: SELECT on SALES tables only (no ANALYTICS, no RESTRICTED)
GRANT SELECT ON TABLE ICEBERG_DEMO_DB.SALES.CUSTOMER_ORDERS TO ROLE DATA_ANALYST;
GRANT SELECT ON TABLE ICEBERG_DEMO_DB.SALES.PRODUCT_CATALOG TO ROLE DATA_ANALYST;

-- ─────────────────────────────────────────────
-- 6. PAT GENERATION
-- ─────────────────────────────────────────────
-- Generate PATs for both service users. Copy and save the token values securely.

ALTER USER IF EXISTS IRC_CLIENT_ENGINEER
  ADD PROGRAMMATIC ACCESS TOKEN ENGINEER_PAT
    DAYS_TO_EXPIRY = 30
    COMMENT = 'Horizon IRC demo - DATA_ENGINEER';

ALTER USER IF EXISTS IRC_CLIENT_ANALYST
  ADD PROGRAMMATIC ACCESS TOKEN ANALYST_PAT
    DAYS_TO_EXPIRY = 30
    COMMENT = 'Horizon IRC demo - DATA_ANALYST';

-- >> IMPORTANT: Copy the PAT values from the output above.
-- >> You will paste them into the PySpark notebook configuration cell.

-- ─────────────────────────────────────────────
-- 7. KEY-PAIR AUTH SETUP (OPTIONAL)
-- ─────────────────────────────────────────────
-- To demo key-pair authentication alongside PAT:
--
-- Step 1: Generate a key pair on your machine:
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
--   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
--
-- Step 2: Assign the public key to the engineer user:
--   ALTER USER IRC_CLIENT_ENGINEER SET RSA_PUBLIC_KEY='<contents of rsa_key.pub without header/footer>';
--
-- Step 3: Generate a JWT using SnowSQL:
--   snowsql --private-key-path rsa_key.p8 --generate-jwt \
--     -h "<account_identifier>.snowflakecomputing.com" \
--     -a "<account_locator>" -u IRC_CLIENT_ENGINEER
--
-- Step 4: Exchange JWT for an access token:
--   curl -X POST "https://<account_id>.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens" \
--     -H 'Content-Type: application/x-www-form-urlencoded' \
--     -d 'grant_type=client_credentials' \
--     -d 'scope=session:role:DATA_ENGINEER' \
--     -d 'client_secret=<JWT_TOKEN>'
--
-- Reference: https://docs.snowflake.com/en/user-guide/tables-iceberg-query-using-external-query-engine-snowflake-horizon#key-pair-authentication

-- ─────────────────────────────────────────────
-- 8. EXTERNAL WRITES — ACCOUNT-LEVEL FLAGS
-- ─────────────────────────────────────────────
-- Required to allow Spark to write to Iceberg tables through Horizon IRC.
-- Without these, external writes will get 403 on s3:PutObject.
-- Uncomment and run if your account supports external writes:

-- ALTER ACCOUNT SET FEATURE_HORIZON_POLARIS_WRITES = 'ENABLED'
--   PARAMETER_COMMENT = 'Enable external writes for Horizon IRC demo';
-- ALTER ACCOUNT SET ENABLE_HORIZON_POLARIS_WRITE_TO_COMMIT_LOG = TRUE
--   PARAMETER_COMMENT = 'Enable write commit log for Horizon IRC demo';
-- ALTER ACCOUNT SET ENABLE_HORIZON_POLARIS_PERSISTENCE_REFRESH = TRUE
--   PARAMETER_COMMENT = 'Enable persistence refresh for Horizon IRC demo';

-- ─────────────────────────────────────────────
-- 9. COMPUTE POOL FOR NOTEBOOK CONTAINER RUNTIME
-- ─────────────────────────────────────────────
CREATE COMPUTE POOL IF NOT EXISTS SPARK_DEMO_POOL
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_S;

GRANT USAGE ON COMPUTE POOL SPARK_DEMO_POOL TO ROLE DATA_ENGINEER;

-- ─────────────────────────────────────────────
-- 10. VALIDATION QUERIES
-- ─────────────────────────────────────────────

-- As Engineer: should see everything, all regions
USE ROLE DATA_ENGINEER;
SELECT 'CUSTOMER_ORDERS (engineer)' AS test, COUNT(*) AS row_count FROM ICEBERG_DEMO_DB.SALES.CUSTOMER_ORDERS;
SELECT 'PRODUCT_CATALOG (engineer)' AS test, COUNT(*) AS row_count FROM ICEBERG_DEMO_DB.SALES.PRODUCT_CATALOG;
SELECT 'USER_PROFILES (engineer)'   AS test, COUNT(*) AS row_count FROM ICEBERG_DEMO_DB.ANALYTICS.USER_PROFILES;
SELECT 'REVENUE_SUMMARY (engineer)' AS test, COUNT(*) AS row_count FROM ICEBERG_DEMO_DB.RESTRICTED.REVENUE_SUMMARY;
SELECT * FROM ICEBERG_DEMO_DB.SALES.CUSTOMER_ORDERS ORDER BY order_id;
SELECT * FROM ICEBERG_DEMO_DB.ANALYTICS.USER_PROFILES ORDER BY user_id;

-- As Analyst: should see only SALES, only US-WEST orders (4 rows)
USE ROLE DATA_ANALYST;
SELECT 'CUSTOMER_ORDERS (analyst)' AS test, COUNT(*) AS row_count FROM ICEBERG_DEMO_DB.SALES.CUSTOMER_ORDERS;
SELECT 'PRODUCT_CATALOG (analyst)' AS test, COUNT(*) AS row_count FROM ICEBERG_DEMO_DB.SALES.PRODUCT_CATALOG;
SELECT * FROM ICEBERG_DEMO_DB.SALES.CUSTOMER_ORDERS ORDER BY order_id;

-- These should FAIL for analyst:
-- SELECT * FROM ICEBERG_DEMO_DB.ANALYTICS.USER_PROFILES;    -- no access
-- SELECT * FROM ICEBERG_DEMO_DB.RESTRICTED.REVENUE_SUMMARY; -- no access
