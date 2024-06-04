use role accountadmin;
grant ownership on database adventureworks to role sysadmin revoke current grants;
grant ownership on database adventureworks_dev to role sysadmin revoke current grants;
grant ownership on all schemas in database adventureworks to role dbt_funcrole revoke current grants;
grant ownership on all tables in database adventureworks to role dbt_funcrole revoke current grants;
grant ownership on all views in database adventureworks to role dbt_funcrole revoke current grants;

grant ownership on all schemas in database adventureworks_dev to role dbt_funcrole revoke current grants;
grant ownership on all tables in database adventureworks_dev to role dbt_funcrole revoke current grants;
grant ownership on all views in database adventureworks_dev to role dbt_funcrole revoke current grants;


use role sysadmin;
create warehouse if not exists compute_etl;
create database if not exists adventureworks;
create database if not exists adventureworks_dev;
create database if not exists adventureworks_preprod;

use role useradmin;
create role if not exists dbt_funcrole;

use role securityadmin;
grant operate, usage on warehouse compute_etl to role dbt_funcrole;

grant usage on database adventureworks_dev to role dbt_funcrole;
grant usage,all on all schemas in database adventureworks_dev to role dbt_funcrole;
grant usage,all on future schemas in database adventureworks_dev to role dbt_funcrole;
grant create schema on database adventureworks_dev to role dbt_funcrole;

grant usage on database adventureworks to role dbt_funcrole;
grant usage,all on all schemas in database adventureworks to role dbt_funcrole;
grant usage,all on future schemas in database adventureworks to role dbt_funcrole;
grant create schema on database adventureworks to role dbt_funcrole;

grant usage on database adventureworks_preprod to role dbt_funcrole;
grant usage,all on all schemas in database adventureworks_preprod to role dbt_funcrole;
grant usage,all on future schemas in database adventureworks_preprod to role dbt_funcrole;
grant create schema on database adventureworks_preprod to role dbt_funcrole;

grant role dbt_funcrole to user networkLevels;