/****************************************************************************************
 Script Name  : DataWarehouse_Recreate.sql
 Purpose      : Drop and recreate the DataWarehouse database, then create required schemas
 Author       : DataPadawan
 Created Date : 2025-03-12

 WARNING:
   - This script will permanently DELETE the database [DataWarehouse] if it exists.
   - All objects and data inside [DataWarehouse] will be lost.
   - Ensure you have the correct approvals and backups before running.
****************************************************************************************/

USE master;
GO

/****************************************************************************************
 Step 1 - Drop the 'DataWarehouse' database if it already exists
   - Forces SINGLE_USER mode to disconnect active sessions
   - Uses ROLLBACK IMMEDIATE to terminate open transactions
****************************************************************************************/
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

/****************************************************************************************
 Step 2 - Create the 'DataWarehouse' database
****************************************************************************************/
CREATE DATABASE DataWarehouse;
GO

/****************************************************************************************
 Step 3 - Create required schemas in the DataWarehouse database
   - bronze : Landing / raw ingestion layer
   - silver : Cleaned / conformed layer
   - gold   : Curated / presentation layer
****************************************************************************************/
USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

