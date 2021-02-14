--
-- DATABASE
--
IF DB_ID('$(dbname)') IS NULL
BEGIN
    CREATE DATABASE $(dbname)
END
GO

use $(dbname)
GO 

-- 
-- SCHEMAS
--
IF NOT EXISTS (SELECT name from sys.schemas WHERE name = 'sample')
BEGIN

	EXEC('CREATE SCHEMA sample')
    
END
GO

--
-- TABLES
--
IF  NOT EXISTS 
	(SELECT * FROM sys.objects 
	 WHERE object_id = OBJECT_ID(N'[sample].[DeviceMeasurement]') AND type in (N'U'))
	 
BEGIN

	CREATE TABLE [sample].[DeviceMeasurement](
        [DeviceID] [NVARCHAR](50) NOT NULL,
        [ParameterID] [NVARCHAR](50) NOT NULL,
        [Timestamp] [DATETIME2],
        [Value] [DECIMAL](18, 2)
    );

END
GO

--
-- INDEXES
--
IF EXISTS (SELECT name FROM sys.indexes WHERE name = N'UX_DeviceMeasurement')
BEGIN
    DROP INDEX [UX_DeviceMeasurement] on [sample].[DeviceMeasurement];
END
GO

CREATE UNIQUE INDEX UX_DeviceMeasurement ON [sample].[DeviceMeasurement](DeviceID, ParameterID, Timestamp);
GO

--
-- STORED PROCEDURES
--
IF OBJECT_ID(N'[sample].[InsertOrUpdateDeviceMeasurements]', N'P') IS NOT NULL
BEGIN
    DROP PROCEDURE [sample].[InsertOrUpdateDeviceMeasurements];
END
GO

IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = 'DeviceMeasurementType')
BEGIN
    DROP TYPE [sample].[DeviceMeasurementType];
END
GO

CREATE TYPE [sample].[DeviceMeasurementType] AS TABLE (
        [DeviceID] [NVARCHAR](50) NOT NULL,
        [ParameterID] [NVARCHAR](50) NOT NULL,
        [Timestamp] [DATETIME2],
        [Value] [DECIMAL](18, 2)
);
GO

CREATE PROCEDURE [sample].[InsertOrUpdateDeviceMeasurements]
  @TVP [sample].[DeviceMeasurementType] ReadOnly
AS
BEGIN
    
    SET NOCOUNT ON;
 
    MERGE [sample].[DeviceMeasurement] AS TARGET USING @TVP AS SOURCE ON (TARGET.DeviceID = SOURCE.DeviceID) AND (TARGET.ParameterID = SOURCE.ParameterID) AND (TARGET.Timestamp = SOURCE.Timestamp)
    WHEN MATCHED THEN
        UPDATE SET TARGET.Value = SOURCE.Value
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (DeviceID, ParameterID, Timestamp, Value)
        VALUES (SOURCE.DeviceID, SOURCE.ParameterID, SOURCE.Timestamp, SOURCE.Value);

END
GO