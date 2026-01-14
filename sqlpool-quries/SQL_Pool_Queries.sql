CREATE MASTER KEY ENCRYPTION BY PASSWORD = <<password>>;

-- CREATING A SCOPE
CREATE DATABASE SCOPED CREDENTIAL storage_cred
WITH IDENTITY = 'Managed Identity';

--DEFINING DATA SOURCE
CREATE EXTERNAL DATA SOURCE gold_data_source
WITH (
    TYPE = HADOOP,
    LOCATION = 'abfss://<<container>>@<<Storageaccount_name>>.dfs.core.windows.net/',
    CREDENTIAL = storage_cred
);

CREATE EXTERNAL FILE FORMAT ParquetFileFormat
WITH ( FORMAT_TYPE = PARQUET );

--CREATE tables
--Patient Dim
CREATE EXTERNAL TABLE dbo.dim_patient (
    patient_id VARCHAR(50),
    gender VARCHAR(10),
    age INT,
    effective_from DATETIME2,
    surrogate_key BIGINT,
    effective_to DATETIME2,
    is_current BIT
)
WITH(
    LOCATION = 'dim_patient/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);


--Dept Dim
CREATE EXTERNAL TABLE dbo.dim_department (
    department NVARCHAR(200),
    hospital_id INT,
    surrogate_key BIGINT
)
WITH(
    LOCATION = 'dim_department/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);

--Fact Table 


CREATE EXTERNAL TABLE dbo.fact_patient_flow (
    fact_id BIGINT,
    patient_sk BIGINT,
    department_sk BIGINT ,
    admission_time DATETIME2,
    discharge_time DATETIME2,
    admission_date DATE,
    length_of_stay_hours FLOAT,
    is_currently_admitted BIT,
    bed_id INT,
    event_ingestion_time DATETIME2
)
WITH(
    LOCATION = 'fact_patient_flow/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);


SELECT TOP 5 * FROM dbo.fact_patient_flow;