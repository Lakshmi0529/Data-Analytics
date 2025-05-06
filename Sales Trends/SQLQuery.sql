/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouseAnalytics' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, this script creates a schema called gold
	
WARNING:
    Running this script will drop the entire 'DataWarehouseAnalytics' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouseAnalytics' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouseAnalytics')
BEGIN
    ALTER DATABASE DataWarehouseAnalytics SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouseAnalytics;
END;
GO

-- Create the 'DataWarehouseAnalytics' database
CREATE DATABASE DataWarehouseAnalytics;
GO

USE DataWarehouseAnalytics;
GO

-- Create Schemas

CREATE SCHEMA gold;
GO

CREATE TABLE gold.dim_customers(
	customer_key int,
	customer_id int,
	customer_number nvarchar(50),
	first_name nvarchar(50),
	last_name nvarchar(50),
	country nvarchar(50),
	marital_status nvarchar(50),
	gender nvarchar(50),
	birthdate date,
	create_date date
);
GO

CREATE TABLE gold.dim_products(
	product_key int ,
	product_id int ,
	product_number nvarchar(50) ,
	product_name nvarchar(50) ,
	category_id nvarchar(50) ,
	category nvarchar(50) ,
	subcategory nvarchar(50) ,
	maintenance nvarchar(50) ,
	cost int,
	product_line nvarchar(50),
	start_date date 
);
GO

CREATE TABLE gold.fact_sales(
	order_number nvarchar(50),
	product_key int,
	customer_key int,
	order_date date,
	shipping_date date,
	due_date date,
	sales_amount int,
	quantity tinyint,
	price int 
);
GO

TRUNCATE TABLE gold.dim_customers;
GO

BULK INSERT gold.dim_customers
FROM 'C:\Users\ghost\Downloads\sql-data-analytics-project\sql-data-analytics-project\datasets\csv-files\gold.dim_customers.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

TRUNCATE TABLE gold.dim_products;
GO

BULK INSERT gold.dim_products
FROM 'C:\Users\ghost\Downloads\sql-data-analytics-project\sql-data-analytics-project\datasets\csv-files\gold.dim_products.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

TRUNCATE TABLE gold.fact_sales;
GO

BULK INSERT gold.fact_sales
FROM 'C:\Users\ghost\Downloads\sql-data-analytics-project\sql-data-analytics-project\datasets\csv-files\gold.fact_sales.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

-- Analyzing sales over time

Select year(order_date) as order_year,
	   month(order_date) as order_month,
	   sum(sales_amount) as total_sales,
	   count(distinct customer_key) as total_customers,
	   sum(quantity) as total_quantity
From gold.fact_sales
where order_date is not null
group by year(order_date),month(order_date)
order by year(order_date),month(order_date)
;

Select Datetrunc(month,order_date) as order_date,
	   sum(sales_amount) as total_sales,
	   count(distinct customer_key) as total_customers,
	   sum(quantity) as total_quantity
From gold.fact_sales
where order_date is not null
group by Datetrunc(month,order_date)
order by Datetrunc(month,order_date)
;
-- calculating total sales per month and
-- the running total of sales over time
select order_date,
	  total_sales,
	  sum(total_sales) over (Order by order_date) as running_total_sales
from (
	select datetrunc(month,order_date) as order_date,sum(sales_amount) as total_sales
	from gold.fact_sales
	where order_date is not null
	group by datetrunc(month,order_date)
	) t
;
-- Performance analysis (comparing current sales Vs Avg sales/previous year sales)
with yearly_product_sales as (
	select year(f.order_date) as order_year,d.product_name,
		   sum(f.sales_amount) as current_sales
	from gold.fact_sales f
	left join gold.dim_products d
	on f.product_key = d.product_key
	where year(f.order_date) is not null
	group by year(f.order_date), d.product_name
		) 
select order_year,
	   product_name,
	   current_sales,
	   avg(current_sales) over (partition by product_name) avg_sales,
	   current_sales - avg(current_sales) over (partition by product_name) diff_avg,
case when current_sales - avg(current_sales) over (partition by product_name) > 0 then 'Above Avg'
	 when current_sales - avg(current_sales) over (partition by product_name) < 0 then 'Below Avg'
	 else 'Avg'
end avg_change,
lag(current_sales) over (partition by product_name order by order_year) py_sales,
current_sales - lag(current_sales) over (partition by product_name order by order_year) as diff_py,
case when current_sales - lag(current_sales) over (partition by product_name order by order_year) > 0 then 'Increase'
	 when current_sales - lag(current_sales) over (partition by product_name order by order_year) < 0 then 'Decrease'
	 else 'No Change'
end py_change
from yearly_product_sales
order by product_name,order_year
;
-- Part-to-Whole analysis
-- which categories contribute most to the overall sales?
with sales_by_category as (
	select category, sum(sales_amount) category_sales
	from gold.fact_sales as f
	left join gold.dim_products p
	on f.product_key = p.product_key
	group by category) 

select category,category_sales,
	   sum(category_sales) over () total_sales,
	   concat(round((cast(category_sales as float)/sum(category_sales) over ())*100,2),'%') percent_of_total
from sales_by_category
group by category,category_sales
order by 2 desc
;
-- Data segmentation
with product_segments as (
select product_key,
	   product_name,
	   cost,
	   case when cost < 100 then 'Below 100'
			when cost Between 100 and 500 then '100-500'
			when cost Between 500 and 1000 then '500-1000'
			else 'Above 1000'
	   end cost_range
from gold.dim_products)

select cost_range, count(product_key) as total_products
from product_segments
group by cost_range
order by 2 desc
;
-- customer segmentation based on spending and activity
-- VIP - atleast 12 months history and spent more than $5000
-- Regular - atleast 12 months history and spent $5000 or less
-- New customer - less than 12 months history

with customer_spending as (
	Select c.customer_key,
		  sum(f.sales_amount) as total_spending,
		  min(f.order_date) as first_order_date,
		  max(f.order_date) as last_order_date,
		  DATEDIFF(month,min(f.order_date),max(f.order_date)) as lifespan,
		  case when DATEDIFF(month,min(f.order_date),max(f.order_date)) >= 12
			   and sum(f.sales_amount) > 5000 then 'VIP'
			   when DATEDIFF(month,min(f.order_date),max(f.order_date)) >= 12
			   and sum(f.sales_amount) <= 5000 then 'Regular'
			   else 'New Customer'
		  end customer_type
	from gold.fact_sales f
	left join gold.dim_customers c
	on f.customer_key = c.customer_key
	group by c.customer_key)

select customer_type, count(customer_key) as total_customers
from customer_spending
group by customer_type
order by 2 desc
;
-- Customer report
CREATE VIEW gold.report_customers AS
with base_query as (
	Select f.order_number,
		   f.product_key,
		   f.order_date,
		   f.sales_amount,
		   f.quantity,
		   c.customer_key,
		   c.customer_number,
		   CONCAT(c.first_name,' ',c.last_name) customer_name,
		   datediff(year,c.birthdate,GETDATE()) age
	from gold.fact_sales f
	left join gold.dim_customers c
	on c.customer_key = f.customer_key
	where f.order_date is not null)

, customer_aggregation as (
select 
	   customer_key,
	   customer_number,
       customer_name,
	   age,
	   count(distinct order_number) as total_orders,
	   sum(sales_amount) as total_sales,
	   sum(quantity) as total_quantity,
	   count(distinct product_key) as total_products,
	   max(order_date) as last_order_date,
	   DATEDIFF(month,min(order_date),max(order_date)) as lifespan
from base_query
group by customer_key,
	   customer_number,
       customer_name,
	   age
)

select 
	   customer_key,
	   customer_number,
       customer_name,
	   age,
	   case when age < 20 then 'Under 20'
			when age between 20 and 29 then '20-29'
			when age between 30 and 39 then '30-39'
			when age between 40 and 49 then '40-49'
			else '50 and Above'
	   end age_group,
	   total_orders,
	   total_sales,
	   total_quantity,
	   total_products,
	   last_order_date,
	   lifespan,
	   case when lifespan >= 12 and total_sales > 5000 then 'VIP'
			when lifespan >= 12 and total_sales <= 5000 then 'Regular'
			else 'New Customer'
	   end customer_type,
	   -- calculating receny
	   DATEDIFF(month,last_order_date,GETDATE()) recency,

	   -- calculating average order value (AOV)
	   case when total_sales = 0 then 0
			else total_sales/total_orders
	   end  as avg_order_value,

	   -- calculating average monthly spend

	   case when lifespan = 0 then 0
			else total_sales/lifespan
	   end as avg_monthly_spend
from customer_aggregation
	   
--- Product report 
 
 create view gold.report_products as
 with base_query as (
 Select f.order_number,
		   f.product_key,
		   f.customer_key,
		   f.order_date,
		   f.sales_amount,
		   f.quantity,
		   p.product_name,
		   p.category,
		   p.subcategory,
		   p.cost
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where order_date is not null
),
product_aggregations as (
-- summarizing aggregations at product level

select	   
		   product_key,
		   product_name,
		   category,
		   subcategory,
		   cost,
		   count(distinct order_number) as total_orders,
		   count(distinct customer_key) as total_customers,
		   sum(sales_amount) as total_sales,
		   sum(quantity) as total_quantity,
		   max(order_date) as last_order_date,
	       DATEDIFF(month,min(order_date),max(order_date)) as lifespan,
		   Round(avg(cast(sales_amount as float)/nullif(quantity,0)),1) as avg_selling_price
from base_query
group by   product_key,
		   product_name,
		   category,
		   subcategory,
		   cost
)
-- final query

select 
	product_key,
	product_name,
	category,
    subcategory,
	cost,
	last_order_date,
	DATEDIFF(month,last_order_date,getdate()) as recency_in_months,
	case when total_sales > 50000 then 'High Performer'
		 when total_sales >= 10000 then 'Mid-Range'
		 else 'Low Performer'
	end as product_segment,
	lifespan,
	total_orders,
	total_sales,
	total_quantity,
	total_customers,
	avg_selling_price,
	-- Average Order Revenue(AOR)

	case when total_orders = 0 then 0
		 else total_sales/total_orders
	end as avg_order_revenue,

	-- Average Monthly Revenue (AMR)

	case when lifespan = 0 then total_sales
		 else total_sales/lifespan
	end as avg_monthly_revenue
from product_aggregations
group by product_key,
	product_name,
	category,
    subcategory,
	cost,
	last_order_date,
	lifespan,
	total_orders,
	total_sales,
	total_quantity,
	total_customers,
	avg_selling_price


select *
from gold.report_products
	



