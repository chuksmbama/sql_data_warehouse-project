

--use master;

-- => CREATING A PROCEDURE FOR FREQUENTLY USED SQL CODE
declare @start_time datetime, @end_time datetime;
exec bronze.load_bronze;
begin try
--create or alter procedure bronze.load_bronze as
--begin

-- => CREATING THE DATABASE

--create database DataWarehouse;
--use DataWarehouse;

-- => CREATING THE SCHEMEAS

--create schema bronze;
--create schema silver;
--create schema gold;



-- => CREATING TABLES

--if OBJECT_ID ('bronze.crm_cust_info', 'U') is not null
--	drop table bronze.crm_cust_info;
--create table bronze.crm_cust_info (
--	cst_id int,
--	cst_key nvarchar(50),
--	cst_firstname nvarchar(50),
--	cst_lastname nvarchar(50),
--	cst_material_status nvarchar(50),
--	cst_gndr nvarchar(50),
--	cst_create_date date
--);

--if OBJECT_ID ('bronze.crm_prd_info', 'U') is not null
--	drop table bronze.crm_prd_info;
--create table bronze.crm_prd_info (
--	prd_id int,
--	prd_key nvarchar(50),
--	prd_nm nvarchar(50),
--	prd_cost int,
--	prd_line nvarchar(50),
--	prd_start_dt datetime,
--	prd_end_dt datetime
--);


--if OBJECT_ID ('bronze.crm_sales_details', 'U') is not null
--	drop table bronze.crm_sales_details;
--create table bronze.crm_sales_details (
--	sls_ord_num nvarchar(50),
--	sls_pro_key nvarchar(50),
--	sls_cust_id int,
--	sls_order_dt int,
--	sls_ship_dt int,
--	sls_due_dt int,
--	sls_sales int,
--	sls_quantity int,
--	sls_price int
--);


--if OBJECT_ID ('bronze.erp_loc_a101', 'u') is not null
--	drop table bronze.erp_loc_a101;
--create table bronze.erp_loc_a101 (
--	cid nvarchar(50),
--	cntry nvarchar(50)
--);


--if OBJECT_ID('bronze.erp_cust_az12', 'u') is not null
--	drop table bronze.erp_cust_az12;
--create table bronze.erp_cust_az12 (
--	cid nvarchar(50),
--	bdate date,
--	gen nvarchar(50)
--);



--if OBJECT_ID ('bronze.erp_px_cat_g1v2', 'u') is not null
--	drop table bronze.erp_px_cat_g1v2;
--create table bronze.erp_px_cat_g1v2 (
--	id nvarchar(50),
--	cat nvarchar(50),
--	subcat nvarchar(50),
--	maintenance nvarchar(50)
--);


-- => LOADING THE DATA INTO THE TABLE
print '============================================================================';
print 'Loading bronze layer';
print '=============================================================================';



print '-----------------------------------------------';
print 'Loading CRM Tables';
print '-----------------------------------------------';

set @start_time = GETDATE();
print '>> Truncating Table: bronze.crm_cust_info'
truncate table bronze.crm_cust_info;

print '>> Inserting data into: bronze.crm_cust_info'
bulk insert bronze.crm_cust_info
from 'C:\Users\personal\OneDrive\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
);
set @end_time = GETDATE();
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';

--select * from bronze.crm_cust_info;
--select count(*) from bronze.crm_cust_info;


print '-----------------------------------------------';
print 'Loading CRM Tables';
print '-----------------------------------------------';

set @start_time = GETDATE();
print '>> Truncating table: bronze.crm_prd_info'
truncate table bronze.crm_prd_info;

print '>> Inserting data into: bronze.crm_prd_info'
bulk insert bronze.crm_prd_info
from 'C:\Users\personal\OneDrive\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
);
set @end_time = GETDATE();
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';

--select * from bronze.crm_prd_info;


print '-----------------------------------------------';
print 'Loading CRM Tables';
print '-----------------------------------------------';

set @start_time = GETDATE();
print '>> Truncating table: bronze.crm_sales_details'
truncate table bronze.crm_sales_details;

print '>> Inserting data into: bronze.crm_sales_details'
bulk insert bronze.crm_sales_details
from 'C:\Users\personal\OneDrive\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
);
set @end_time = GETDATE();
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';


--select * from bronze.crm_sales_details;


print '-----------------------------------------------';
print 'Loading ERP Tables';
print '-----------------------------------------------';

set @start_time = GETDATE();
print '>> Truncating table: bronze.erp_cust_az12'
truncate table bronze.erp_cust_az12;

print '>> Inserting data into: bronze.erp_cust_az12'
bulk insert bronze.erp_cust_az12
from 'C:\Users\personal\OneDrive\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
);
set @end_time = GETDATE();
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';

--select * from bronze.erp_cust_az12;



print '-----------------------------------------------';
print 'Loading ERP Tables';
print '-----------------------------------------------';

set @start_time = GETDATE();
print '>> Truncating Table: bronze.erp_loc_a101'
truncate table bronze.erp_loc_a101;

print '>> Inserting data into: bronze.erp_loc_a101'
bulk insert bronze.erp_loc_a101
from 'C:\Users\personal\OneDrive\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
);
set @end_time = GETDATE();
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';


--select * from bronze.erp_loc_a101;


print '-----------------------------------------------';
print 'Loading ERP Tables';
print '-----------------------------------------------';

set @start_time = GETDATE();
print '>> Truncating Table: bronze.erp_px_cat_g1v2'
truncate table bronze.erp_px_cat_g1v2;

print '>> Inserting data into: bronze.erp_px_cat_g1v2'
bulk insert bronze.erp_px_cat_g1v2
from 'C:\Users\personal\OneDrive\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
with (
	firstrow = 2,
	fieldterminator = ',',
	tablock
);
set @end_time = GETDATE();
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';


--select * from bronze.erp_px_cat_g1v2;
end try
begin catch
	print '================================================'
	print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
	print 'ERROR MESSAGE' + error_message();
	print 'ERROR MESSGAE' + cast(error_number() as nvarchar);
	print '================================================'
end catch
--end
