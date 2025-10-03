/**/

/*-----------------------------------------
              crm_cust_info  
-----------------------------------------*/
select *, row_number() over(partition by cust_id order by cust_create_date desc) as last_flag
from Bronze.crm_cust_info 
where cust_id = 29466;

select *, row_number() over(partition by cust_id order by cust_create_date desc) as last_flag
from Bronze.crm_cust_info 

select * from(
	select *, row_number() over(partition by cust_id order by cust_create_date desc) as last_flag
	from Bronze.crm_cust_info
)t where last_flag != 1;

select * from(
	select *, row_number() over(partition by cust_id order by cust_create_date desc) as last_flag
	from Bronze.crm_cust_info
)t where last_flag = 1 and cust_id = 29466;

-------------------------------------------------

select * from Bronze.crm_cust_info;

select 
	cust_id, cust_key,
	trim(cust_firstname) as cust_firstname,
	trim(cust_lastname) as cust_lastname,
	cust_material_status, 
	case when upper(trim(cust_gndr)) = 'F' then 'Female'
		 when upper(trim(cust_gndr)) = 'M' then 'Male'
		 else 'N/A'
	end cust_gndr,
	cust_create_date
from(
	select *, row_number() over(partition by cust_id order by cust_create_date desc) as last_flag
	from Bronze.crm_cust_info
	where cust_id is not null
)t where last_flag = 1;

-------------------------------------------------

select cust_id, count(*)
from silver.crm_cust_info 
group by cust_id
having count(*) > 1 or cust_id is null;

-------------------------------------------------

select cust_firstname from Silver.crm_cust_info;

select cust_firstname 
from Silver.crm_cust_info
where cust_firstname != trim(cust_firstname);

select cust_lastname 
from Silver.crm_cust_info
where cust_lastname != trim(cust_lastname);

select cust_gndr 
from Silver.crm_cust_info
where cust_gndr != trim(cust_gndr);

select cust_material_status
from Silver.crm_cust_info
where cust_material_status != trim(cust_material_status);

-------------------------------------------------

select distinct cust_gndr from Silver.crm_cust_info;

select distinct cust_material_status from Silver.crm_cust_info;

/*-----------------------------------------
              crm_prd_info  
-----------------------------------------*/

where replace (SUBSTRING(prd_key, 1, 5), '-', '_') not in (

	select distinct id from Bronze.erp_px_cat_g1v2	

);

where SUBSTRING(prd_key, 7, len(prd_key)) not in (

	select sls_prd_key from Bronze.crm_sales_details

);

select
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_date,
	lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) - 1 as prd_end_date_test
from Bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

/*-----------------------------------------
              crm_sales_details
-----------------------------------------*/

select 
	sls_ord_num
from Bronze.crm_sales_details
where sls_ord_num != TRIM(sls_ord_num);

-------------------------------------------------------------------------------------------------------------------------------------

select 
	sls_prd_key
from Bronze.crm_sales_details
where sls_prd_key not in (select cat_key from Silver.crm_prd_info);

select 
	sls_cust_id
from Bronze.crm_sales_details
where sls_cust_id not in (select cust_id from Silver.crm_cust_info);

-------------------------------------------------------------------------------------------------------------------------------------

select 
	nullif (sls_order_dt,0) as sls_order_dt
from Bronze.crm_sales_details
where 
	sls_order_dt <= 0 or 

	LEN(sls_order_dt) != 8 or 
	
	sls_order_dt > 20500101 or 
	sls_order_dt < 19000101;

-------------------------------------------------------------------------------------------------------------------------------------

select 
	nullif (sls_ship_dt,0) as sls_ship_dt
from Bronze.crm_sales_details
where 
	sls_ship_dt <= 0 or 

	LEN(sls_ship_dt) != 8 or 
	
	sls_ship_dt > 20500101 or 
	sls_ship_dt < 19000101;

-------------------------------------------------------------------------------------------------------------------------------------

select 
	nullif (sls_due_dt,0) as sls_due_dt
from Bronze.crm_sales_details
where 
	sls_due_dt <= 0 or 

	LEN(sls_due_dt) != 8 or 
	
	sls_due_dt > 20500101 or 
	sls_due_dt < 19000101;

-------------------------------------------------------------------------------------------------------------------------------------

select * 
from Bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt 

-------------------------------------------------------------------------------------------------------------------------------------

select distinct
	sls_sales,
	sls_quantity,
	sls_price
from Bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0; 

/*-----------------------------------------
              erp_cust_az12
-----------------------------------------*/ 

select 
	cid,
	bdate,
	gen
from Bronze.erp_cust_az12;


select * from Silver.crm_cust_info;

--------------------------

select 
	cid,
	case
		when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
		else cid
	end as cid,
	bdate,
	gen
from Bronze.erp_cust_az12
where case 
		when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
		else cid
	end not in (
		select distinct cust_key from Silver.crm_cust_info
	);
	
select * from Silver.crm_cust_info;

--------------------------

select distinct bdate 
from Bronze.erp_cust_az12
where bdate < '1930-01-01' or bdate > getdate();

--------------------------

select 
	case
		when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
		else cid
	end as cid,
	case
		when bdate > getdate() then NULL
		else bdate
	end as bdate,
	gen
from Bronze.erp_cust_az12
where bdate < '1930-01-01' or bdate > getdate();

--------------------------

select distinct gen from Bronze.erp_cust_az12;

--------------------------

select distinct 
	gen,
	case 
		when upper(trim(gen)) in ('F','FEMALE') then 'Female'
		when upper(trim(gen)) in ('M','MALE') then 'Male'
		else 'N/A'
	end as gen
	from Bronze.erp_cust_az12;

select 
	cid,
	bdate,
	gen
from Silver.erp_cust_az12;


select * from Silver.crm_cust_info;

--------------------------

select distinct bdate 
from Silver.erp_cust_az12
where bdate < '1930-01-01' or bdate > getdate();

--------------------------

select distinct gen from Silver.erp_cust_az12;

--------------------------

select * from Silver.erp_cust_az12;

/*-----------------------------------------
              crm_sales_details
-----------------------------------------*/

select 
	cid,
	cntry
from Bronze.erp_loc_a101;

select 
	cust_key
from Silver.crm_cust_info;

--------------------------

select 
	replace(cid, '-', '') as cid,
	cntry
from Bronze.erp_loc_a101
where replace(cid, '-', '') not in (

	select 
		cust_key
	from Silver.crm_cust_info

);

--------------------------

select distinct 
	cntry as old_cntry,
	case
		when trim(cntry) = 'DE' then 'Germany'
		when trim(cntry) in ('US', 'USA') then 'United States'
		when trim(cntry) = '' or trim(cntry) is NULL then 'N/A'
		else trim(cntry)
	end as cntry
from Bronze.erp_loc_a101
order by old_cntry;

select distinct
	cntry
from Silver.erp_loc_a101;

select * from Silver.erp_loc_a101;

/*-----------------------------------------
              erp_px_cat_g1v2
-----------------------------------------*/

select 
	id,
	cat,
	subcat,
	maintenance
from Bronze.erp_px_cat_g1v2;

select 
	cat_id 
from Silver.crm_prd_info;

--------------------------

select 
	id
from Bronze.erp_px_cat_g1v2
where id not in (
	
	select 
	cat_id 
from Silver.crm_prd_info

);

--------------------------

select 
	id,
	cat,
	subcat,
	maintenance
from Bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance);


select distinct
	cat
from Bronze.erp_px_cat_g1v2;

select distinct
	subcat
from Bronze.erp_px_cat_g1v2;

select distinct
	maintenance
from Bronze.erp_px_cat_g1v2;
	
select * from Silver.erp_px_cat_g1v2;
