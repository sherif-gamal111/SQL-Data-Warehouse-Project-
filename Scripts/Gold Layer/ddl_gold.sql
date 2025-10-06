/*
==================================================================================================================================================================
                                                                  DDL Script --> Create Gold Views
==================================================================================================================================================================
Script Purpose:
    This script creates views for the Gold Layer in the data warehouse.
	The gold layer represents the final dimension and fact tables (Star Schema).

	Each view performs transformations and combines data from the silver layer to  produce a clean, enriched, and business-ready dataset. 

Usage:
	- These views can be queried directly for analytics and reporting.
==================================================================================================================================================================    
*/  

if object_id ('gold.dim_customers', 'v') is not null
	 drop table gold.dim_customers;
create view gold.dim_customers 
as
select 
	row_number() over(order by cust_id) as customer_key,
	ci.cust_id as customer_id,
	ci.cust_key as customer_number,
	ci.cust_firstname as first_name,
	ci.cust_lastname as last_name,
	la.cntry as country,
	ci.cust_material_status as material_status,
	ca.bdate as birth_date,
	ci.cust_create_date as create_date,
	case 
		when ci.cust_gndr != 'N/A' then ci.cust_gndr
		else coalesce(ca.gen, 'N/A') 
	end as gender
from Silver.crm_cust_info ci
left join Silver.erp_cust_az12 ca on ci.cust_key = ca.cid 
left join Silver.erp_loc_a101 la on ci.cust_key = la.cid;

------------------------

if object_id ('gold.dim_products', 'v') is not null
	 drop table gold.dim_products;
create view gold.dim_products
as
select 
	row_number() over(order by cp.prd_start_dt , cp.cat_key) as product_key,
	cp.prd_id as product_id,
	cp.cat_key as product_number,
	cp.prd_nm as product_name,
	cp.cat_id as category_id,
	ep.cat as category,
	ep.subcat as subcategory,
	ep.maintenance,
	cp.prd_cost as cost,
	cp.prd_line as product_line,
	cp.prd_start_dt as start_date
from Silver.crm_prd_info cp
left join Silver.erp_px_cat_g1v2 ep on cp.cat_id = ep.id
where prd_end_date is null;

------------------------

if object_id ('gold.fact_sales', 'v') is not null
	 drop table gold.fact_sales;
create view gold.fact_sales
as
select 
	sls_ord_num as order_number,
	gp.product_key,
	gc.customer_key,
	sls_order_dt as order_date,
	sls_ship_dt as shiping_date,
	sls_due_dt as due_date,
	sls_sales as sales_amount,
	sls_quantity as quantity,
	sls_price as price
from Silver.crm_sales_details cs
left join gold.dim_products gp on cs.sls_prd_key = gp.product_number
left join gold.dim_customers gc on cs.sls_cust_id = gc.customer_id;
