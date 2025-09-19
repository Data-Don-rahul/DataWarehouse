  

   
   -- exec bronze.sp_bronze_load
   CREATE OR ALTER PROC bronze.sp_bronze_load 
   AS
   BEGIN
      BEGIN TRY
			   DECLARE @start_time date, @end_time date, @start_batch date, @end_batch date;
			   PRINT '=============================';
			   PRINT 'Loading Bronze Layer';
			   PRINT '=============================';


			   PRINT'--------------';
			   PRINT 'Loading CRM Tables';
			   PRINT'--------------';
			   SET @start_batch = GETDATE()

			   SET @start_time = GETDATE()
			   PRINT'>>Truncating Table: bronze.crm_cust_info ';
			   TRUNCATE TABLE bronze.crm_cust_info;
			   PRINT'>>Inserting Data In Table: bronze.crm_cust_info ';
			   BULK INSERT bronze.crm_cust_info
			   FROM 'D:\SQL\New folder\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			   WITH(
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
			   );
	           SET @end_time = GETDATE()
			   PRINT 'Time duration to truncaate and Load file bronze.crm_cust_info :- ' + CAST (DATEDIFF(second, @start_time, @end_time) AS VARCHAR)+ ' seconds'
			   PRINT ' '

			   SET @start_time = GETDATE()
			   PRINT'>>Truncating Table: bronze.crm_prd_info ';
			   TRUNCATE TABLE bronze.crm_prd_info;
			   PRINT'>>Inserting Data In Table: bronze.crm_prd_info ';
			   BULK INSERT bronze.crm_prd_info
			   FROM 'D:\SQL\New folder\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			   WITH (
					 FIRSTROW = 2,
					 FIELDTERMINATOR = ',',
					 TABLOCK
			   );   
			   SET @end_time = GETDATE()
			   PRINT 'Time Duration To Truncate And Load File bronze.crm_prd_info  ' + CAST (DATEDIFF (second, @start_time, @end_time) AS VARCHAR) + ' Seconds'
			    PRINT ' '


			   SET @start_time = GETDATE()
			   PRINT'>>Truncating Table: bronze.crm_sales_details ';
			   TRUNCATE TABLE bronze.crm_sales_details;
			   PRINT'>>Inserting Data In Table: bronze.crm_sales_details ';
			   BULK INSERT bronze.crm_sales_details
			   FROM 'D:\SQL\New folder\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			   WITH (
					 FIRSTROW = 2,
					 FIELDTERMINATOR = ',',
					 TABLOCK
			   );
			   SET @end_time = GETDATE()
			   PRINT 'Time duration to truncaate and Load file bronze.crm_sales_details :- ' + CAST (DATEDIFF(second, @start_time, @end_time) AS VARCHAR)+ ' seconds'
			    PRINT ' '


			   SET @end_batch = GETDATE()
			   PRINT 'Time To Load CRM Batch :- ' + CAST ( DATEDIFF (second, @start_batch, @end_batch) AS VARCHAR ) + ' Seconds'


			   PRINT'--------------';
			   PRINT 'Loading ERP Tables';
			   PRINT'--------------';
			   SET @start_batch = GETDATE()

			   SET @start_time = GETDATE()
			   PRINT'>>Truncating Table: bronze.erp_cust_az12 ';
			   TRUNCATE TABLE bronze.erp_cust_az12;
			   PRINT'>>Inserting Data In Table: bronze.erp_cust_az12 ';
			   BULK INSERT bronze.erp_cust_az12
			   FROM 'D:\SQL\New folder\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			   WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
			   );
			   SET @end_time = GETDATE()
			   PRINT 'Time duration to truncaate and Load file bronze.erp_cust_az12 :- ' + CAST (DATEDIFF(second, @start_time, @end_time) AS VARCHAR)+ ' seconds'
	            PRINT ' '

	           
			   SET @start_time = GETDATE()
			   PRINT'>>Truncating Table: bronze.erp_loc_a101 ';
			   TRUNCATE TABLE bronze.erp_loc_a101;
			   PRINT'>>Inserting Data In Table: bronze.erp_loc_a101 ';
			   BULK INSERT bronze.erp_loc_a101
			   FROM 'D:\SQL\New folder\sql-data-warehouse-project\datasets\source_erp\LOC_A101.CSV'
			   WITH (
					 FIRSTROW = 2,
					 FIELDTERMINATOR = ',',
					 TABLOCK
			   );
			   SET @end_time = GETDATE()
			   PRINT 'Time duration to truncaate and Load file bronze.erp_loc_a101 :- ' + CAST (DATEDIFF(second, @start_time, @end_time) AS VARCHAR)+ ' seconds'
			    PRINT ' '


			   SET @start_time = GETDATE()
			   PRINT '>>Truncating Table: bronze.erp_px_cat_g1v2 ';
			   TRUNCATE TABLE bronze.erp_px_cat_g1v2;
			   PRINT'>>Inserting Data In Table: bronze.erp_px_cat_g1v2 ';
			   BULK INSERT bronze.erp_px_cat_g1v2
			   FROM 'D:\SQL\New folder\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			   WITH (
					 FIRSTROW = 2,
					 FIELDTERMINATOR = ',',
					 TABLOCK
			    );
			   SET @end_time = GETDATE()
			   PRINT 'Time duration to truncaate and Load file bronze.erp_px_cat_g1v2 :- ' + CAST (DATEDIFF(second, @start_time, @end_time) AS VARCHAR)+ ' seconds'
			    PRINT ' '


			   SET @end_batch = GETDATE()
			   PRINT 'Time To Load ERP Batch :- ' + CAST ( DATEDIFF (second, @start_batch, @end_batch) AS VARCHAR ) + ' Seconds'


      END TRY
	  BEGIN CATCH
	   PRINT '=====================';
	   PRINT 'ERROR OCCUR DURING BRONZE LAYER LOADING';
	   PRINT 'Erro Message :- ' + ERROR_MESSAGE();
	   PRINT 'Erro Number :- ' + CAST(ERROR_NUMBER() AS VARCHAR);
	   PRINT '=====================';
	  END CATCH
   END;

  
