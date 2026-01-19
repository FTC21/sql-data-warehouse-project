/* ============================================================================
   SCRIPT : DDL – Create Bronze Tables
   PURPOSE: Define and initialize Bronze layer tables for CRM and ERP sources.
   SCOPE  : Tables store raw source data and are designed for full-load ingestion
            prior to downstream Silver and Gold transformations.
   ============================================================================ */

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @begin_procedure DATETIME, @end_procedure DATETIME;
    BEGIN TRY
        SET @begin_procedure = GETDATE();
        PRINT '=====================================';
        PRINT 'Loading Bronze Layer';
        PRINT '=====================================';

        -- -------------------------------------------------------
        -- FULL LOAD DATA CRM
        -- -------------------------------------------------------
        PRINT '-------------------------------------';
        PRINT 'Loading CRM TABLE';
        PRINT '-------------------------------------';

        -- Customer Info 
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info 
        FROM 'C:\Users\Fabrice\Documents\SQL Course\Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW        = 2,
            FIELDTERMINATOR = ',',
            TABLOCK 
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(ms,@start_time, @end_time) AS NVARCHAR) + ' ms';
        PRINT '-------------------';

        -- Product Info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info 
        FROM 'C:\Users\Fabrice\Documents\SQL Course\Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW        = 2, 
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(ms,@start_time, @end_time) AS NVARCHAR) + ' ms';
        PRINT '-------------------';

        -- Sales Details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details 
        FROM 'C:\Users\Fabrice\Documents\SQL Course\Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH ( 
            FIRSTROW        = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(ms,@start_time, @end_time) AS NVARCHAR) + ' ms';
        PRINT '-------------------';
        -- -------------------------------------------------------
        -- FULL LOAD DATA ERP 
        -- -------------------------------------------------------
        PRINT '-------------------------------------';
        PRINT 'Loading ERP TABLE';
        PRINT '-------------------------------------';

        -- Customer Details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12 
        FROM 'C:\Users\Fabrice\Documents\SQL Course\Projects\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW        = 2, 
            FIELDTERMINATOR = ',', 
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(ms,@start_time, @end_time) AS NVARCHAR) + ' ms';
        PRINT '-------------------';

        -- Localisation Details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101; 

        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101 
        FROM 'C:\Users\Fabrice\Documents\SQL Course\Projects\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH ( 
            FIRSTROW        = 2, 
            FIELDTERMINATOR = ',', 
            TABLOCK 
        );
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(ms, @start_time,@end_time) AS NVARCHAR) + ' ms';
        PRINT '-------------------';

        -- Product Categories Details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2; 

        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2 
        FROM 'C:\Users\Fabrice\Documents\SQL Course\Projects\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW        = 2, 
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(ms, @start_time,@end_time) AS NVARCHAR) + ' ms';
        SET @end_procedure = GETDATE();
        PRINT '====================';
        PRINT '>>> Procedure Loading Duration: ' + CAST(DATEDIFF(second, @begin_procedure,@end_procedure) AS NVARCHAR) + ' second';
        PRINT '====================';
    END TRY

    BEGIN CATCH 
        SET @begin_procedure = GETDATE();
        PRINT '=============================='
        PRINT 'ERROR OCCURED DURING LOADING';
        PRINT 'Error Message ' + ERROR_MESSAGE();
        PRINT 'Error Code ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=============================='
        SET @begin_procedure = GETDATE();
        PRINT '====================';
        PRINT '>>> Procedure Loading Duration: ' + CAST(DATEDIFF(second, @begin_procedure,@end_procedure) AS NVARCHAR) + ' second';
        PRINT '====================';
    END CATCH 
END
