/*
  
*/

create or alter procedure Silver.load_data
as
begin

	print 'Truncating table -->  Silver.crm_cust_info'; 
	truncate table Silver.crm_cust_info;
	print 'Inserting Data Into -->  Silver.crm_cust_info';
	insert into silver.crm_cust_info(
		cust_id,
		cust_key,
		cust_firstname,
		cust_lastname,
		cust_material_status,
		cust_gndr,
		cust_create_date
	)

	select 
		cust_id, cust_key,
		trim(cust_firstname) as cust_firstname,
		trim(cust_lastname) as cust_lastname,
		case when upper(trim(cust_material_status)) = 'S' then 'Single'
			 when upper(trim(cust_material_status)) = 'M' then 'Married'
			 else 'N/A'
		end cust_material_status,
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

	----------------------------

	print 'Truncating table -->  Silver.crm_prd_info';
	truncate table Silver.crm_prd_info;
	print 'Inserting Data Into -->  Silver.crm_prd_info';
	insert into Silver.crm_prd_info(
		prd_id,
		cat_id,
		cat_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_date
	)

	select 
		prd_id,
		replace (SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
		SUBSTRING(prd_key, 7, len(prd_key)) as cat_key,
		prd_nm,
		isnull(prd_cost, 0) as prd_cost, 
		case upper(trim(prd_line))
			when 'M' then 'Mountain'
			when 'R' then 'Road'
			when 'S' then 'Other Sales'
			when 'T' then 'Touring'
			else 'N/A'
		end as prd_line,
		cast(prd_start_dt as date) as prd_start_dt,
		cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) - 1 as date) as prd_start_dt 
	from Bronze.crm_prd_info;

	----------------------------

	print 'Truncating table -->  Silver.crm_sales_details';
	truncate table Silver.crm_sales_details;
	print 'Inserting Data Into -->  Silver.crm_sales_details';
	insert into Silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)

	select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case 
			when sls_order_dt <= 0 or LEN(sls_order_dt) != 8 then NULL 
			else cast(cast(sls_order_dt as varchar) as date) 
		end as sls_order_dt,
		case 
			when sls_ship_dt <= 0 or LEN(sls_ship_dt) != 8 then NULL
			else cast(cast(sls_ship_dt as varchar) as date) 
		end as sls_ship_dt, 
		case
			when sls_due_dt <= 0 or LEN(sls_due_dt) != 8 then NULL
			else cast(cast(sls_due_dt as varchar) as date) 
		end as sls_due_dt,
		case 
			when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price) then sls_quantity * abs(sls_price) 
			else sls_sales
		end as sls_sales,
		sls_quantity,
		case 
			when sls_price is null or sls_price <= 0 then sls_sales / nullif(sls_quantity, 0) 
			else sls_price 
		end as sls_price
	from Bronze.crm_sales_details;

	----------------------------

	print 'Truncating table -->  Silver.erp_cust_az12';
	truncate table Silver.erp_cust_az12;
	print 'Inserting Data Into -->  Silver.erp_cust_az12';
	insert into Silver.erp_cust_az12(
		cid,
		bdate,
		gen
	)

	select 
		case 
			when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
			else cid
		end as cid,
		case 
			when bdate > getdate() then NULL
			else bdate
		end as bdate,
		case 
			when upper(trim(gen)) in ('F','FEMALE') then 'Female'
			when upper(trim(gen)) in ('M','MALE') then 'Male'
			else 'N/A'
		end as gen
	from Bronze.erp_cust_az12;

	----------------------------

	print 'Truncating table -->  Silver.erp_loc_a101';
	truncate table Silver.erp_loc_a101;
	print 'Inserting Data Into -->  Silver.erp_loc_a101';
	insert into Silver.erp_loc_a101(
		cid,
		cntry
	)

	select 
		replace(cid, '-', '') as cid, 
		case 
			when trim(cntry) = 'DE' then 'Germany'
			when trim(cntry) in ('US', 'USA') then 'United States'
			when trim(cntry) = '' or trim(cntry) is NULL then 'N/A'
			else trim(cntry)
		end as cntry
	from Bronze.erp_loc_a101;

	----------------------------

	print 'Truncating table -->  Silver.erp_px_cat_g1v2';
	truncate table Silver.erp_px_cat_g1v2;
	print 'Inserting Data Into -->  Silver.erp_px_cat_g1v2';
	insert into Silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
	)

	select 
		id,
		cat,
		subcat,
		maintenance
	from Bronze.erp_px_cat_g1v2;
end

exec Silver.load_data;
