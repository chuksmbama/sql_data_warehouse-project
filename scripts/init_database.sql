/*
Create Database and schemas

script purpose:
   This script creates a new database named 'Datawarehouse' after checking if it already exists.
   If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
   within the database: 'bronze', 'silver' and 'gold'

Warning:
    Running this script will drop the entire 'warehouse' database if it exists.
    All data in the database will be permanently deleted. Proceed with caution and 
    ensure you have proper backups before running this script.
   
*/
use master;
GO
-- Drop and recreate the 'Datawarehouse' databse
IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO


-- Create the 'DataWarehouse' database
create database DataWarehouse;
GO
use DataWarehouse;
GO


-- Create Schemas
create schema bronze;
GO
create schema silver;
GO
create schema gold;
GO
