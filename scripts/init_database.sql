/*
=================================
Create Database and Schemas
================================
Script Purpsse :-> 
    This scripts creates a new database named 'DataWarehouse' after checking if it already exist.
    If the datavase exists, it is dropped and recreated. Additionally, the script sets up three sxhemas within
    the database : 'bronze', 'silver', 'gold'.

WARNING:
   Running ehis scrits will drop the entire 'DataWarehouse' database if it exists.
   All data in the database will be permanently deleted.Proceed with caution
   and ensure you have prper backups before runnng this script.
*/

--Drop and recreate the 'DataWarehouse'
IF EXISTS (SELECT 1 FROM sys.database WHERE name = 'DataWarehouse')
  BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITHACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
  END;

--Creating the 'DataWarehouse' databse
CREATE DATABASE DataWarehouse

  --Creating Schemas
   CREATE SCHEMA bronze;
   go
   CREATE SCHEMA silver;
   go
   CREATE SCHEMA gold;
