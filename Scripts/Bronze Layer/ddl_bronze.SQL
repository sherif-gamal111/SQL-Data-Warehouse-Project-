/*
==================================================================================================================================================================
                                                                  DDL Script --> Create Bronze Tables
==================================================================================================================================================================
Script Purpose:
    This script creates tables in the Bronze Schema, at the first query, checks if the table exists, drops it if not creates one.
    Run this script to redefine the DDL structure of Bronze tables.
==================================================================================================================================================================    
*/

if object_id ('Bronze.crm_cust_info', 'u') is not null
	 drop table Bronze.crm_cust_info;
create table Bronze.crm_cust_info(
	cust_id int,
	cust_key nvarchar(50),
	cust_firstname nvarchar(50),
	cust_lastname nvarchar(50),
	cust_material_status nvarchar(50),
	cust_gndr nvarchar(50),
	cust_create_date date
);

if object_id ('Bronze.crm_prd_info', 'u') is not null
	 drop table Bronze.crm_prd_info;
create table Bronze.crm_prd_info(
	prd_id int,
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt datetime,
	prd_end_date datetime
);

if object_id ('Bronze.crm_sales_details', 'u') is not null
	 drop table Bronze.crm_sales_details;
create table Bronze.crm_sales_details(
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int
);

if object_id ('Bronze.erp_cust_az12', 'u') is not null
	 drop table Bronze.erp_cust_az12;
create table Bronze.erp_cust_az12(
	cid nvarchar(50),
	bdate date,
	gen nvarchar(50)
);

if object_id ('Bronze.erp_loc_a101', 'u') is not null
	 drop table Bronze.erp_loc_a101;
create table Bronze.erp_loc_a101(
	cid nvarchar(50),
	cntry nvarchar(50)
);

if object_id ('Bronze.erp_px_cat_g1v2', 'u') is not null
	 drop table Bronze.erp_px_cat_g1v2;
create table Bronze.erp_px_cat_g1v2(
	id nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintenance nvarchar(50)
);
