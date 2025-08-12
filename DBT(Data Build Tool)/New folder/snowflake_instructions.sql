
--- Creating warehouse with it's size 
CREATE WAREHOUSE dbt_wh WITH WAREHOUSE_SIZE='x-small';
--- Create database  in Warehouse 
CREATE DATABASE IF NOT EXISTS dbt_db;
--- Creating our own role for the data warehouse 
CREATE ROLE IF NOT EXISTS dbt_role;

SHOW GRANTS ON WAREHOUSE dbt_wh;

--- Granting permission to certain roles 
GRANT usage ON WAREHOUSE dbt_wh TO ROLE dbt_role;
--- Grant permission to user 
GRANT ROLE dbt_role TO USER om;
GRANT ALL ON DATABASE dbt_db TO dbt_role;

USE ROLE dbt_role;