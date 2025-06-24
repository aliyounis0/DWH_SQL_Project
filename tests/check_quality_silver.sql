/*
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.
*/


--check data quality and transformations :

--crm_cust_info

--check duplicated rows and nulls in PK :
select cst_id ,count(*) as flag
from bronze.crm_cust_info 
group by cst_id
having count(*) > 1 or cst_id is null ; 


--check unwanted spaces in the values : 
select cst_firstname 
from bronze.crm_cust_info 
where cst_firstname !=trim(cst_firstname)

select cst_lastname 
from bronze.crm_cust_info 
where cst_lastname !=trim(cst_lastname)

--Data standardizations & consistency :
select distinct cst_gndr 
from bronze.crm_cust_info 


select distinct cst_marital_status 
from bronze.crm_cust_info 
------------------------------------------------------------------

--crm_prd_info

--check duplicated rows and nulls in PK :
select prd_id , count(*)
from bronze.crm_prd_info
group by prd_id 
having count(*) > 1 or prd_id is null --(NO result..)

--check unwanted spaces in the values : 
select prd_nm 
from bronze.crm_prd_info
where prd_nm != trim(prd_nm) --(No result)

--check null or negative values:
select prd_cost
from bronze.crm_prd_info
where prd_cost< 0 or prd_cost is null

--Data standardizations & consistency :
select distinct prd_line 
from bronze.crm_prd_info

select prd_start_dt ,prd_end_dt
from bronze.crm_prd_info
where prd_start_dt  > prd_end_dt
----------------------------------------------------------------------------------

--crm_sales_details
--check the invalid dates :
select sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 
or len(sls_order_dt) < 8
or sls_order_dt > 20500101
or sls_order_dt < 19000101

select sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0 
or len(sls_due_dt) < 8
or sls_due_dt > 20500101
or sls_due_dt < 19000101

select sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt <= 0 
or len(sls_ship_dt) < 8
or sls_ship_dt > 20500101
or sls_ship_dt < 1900010

select sls_order_dt 
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

--crm_sales_details 
--check data consistency : Between sales , quantity and price :

select sls_sales , sls_quantity , sls_price 
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0

-------------------------------------------------------------------
--erp_cust_az12

--identity Out_of Range Dates :

select bdate 
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate  > GETDATE()


--Date standardization & consistency :
select distinct gen 
from bronze.erp_cust_az12

--------------------------------------------------
--erp_loc_a101
--Date standardization & consistency :
select distinct cntry 
from bronze.erp_loc_a101

---------------------------------------------------

--erp_px_cat_g1v2
--check unwanted spaces in the values : 

select * from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)

--Date standardization & consistency :

select distinct cat from bronze.erp_px_cat_g1v2
select distinct subcat from bronze.erp_px_cat_g1v2
select distinct maintenance from bronze.erp_px_cat_g1v2
