/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/
use master;
GO

--Drop and recreate the 'Datawarehouse' database
if EXISTS (SELECT 1 FROM sys.databases where name ='Datawarehouse')
BEGIN
	ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE Datawarehouse;
END;
GO

--Create the 'Datawarehouse' Database
Create Database DataWarehouse;
GO

use DataWarehouse ;
GO

--Create schema
Create schema bronze ;
GO

Create schema silver ;
GO

Create schema gold ;
GO
