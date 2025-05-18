
--Create Database and Schemas 

USE master;

-- drop and recreate the database if exist
IF EXISTS (SELECT 1 FROM sys.databases WHERE NAME= 'Datawarehouse')
BEGIN
    ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE Datawarehouse;
END;
GO

-- CREATING THE DATAWAREHOUSE DATABASE
CREATE DATABASE Datawarehouse;
GO

USE Datawarehouse;
GO

--Creating the schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

