-- Inspecting data
SELECT *
FROM sales_data
;


-- Checking unique values
select distinct status from sales_data
select distinct YEAR_ID from sales_data
select distinct PRODUCTLINE from sales_data
select distinct COUNTRY from sales_data
select distinct STATE from sales_data
select distinct DEALSIZE from sales_data
select distinct TERRITORY from sales_data

-- Analysis
---- Let's start by grouping sales by productline

Select convert(int,SUM(sales)) as Total_Sales_By_Productline, productline
From sales_data
Group by productline
order by 1 desc
;

-- Grouping sales by year

Select  year_id, convert(int,SUM(sales)) as Total_Sales_By_Year
From sales_data
Group by year_id
order by 2 desc
;

-- Investigating the reason for lower sales in 2005

select count(distinct month_id) from sales_data
where year_id = 2005;

-- Grouping sales by dealsize

Select  dealsize, convert(int,SUM(sales)) as Total_Sales
From sales_data
Group by dealsize
order by 2 desc
;


-- Best month for sales in a specific year
Select  month_id, convert(int,SUM(sales)) as Total_Sales, count(ordernumber) as Frequency
From sales_data
where year_id = 2005 -- change year_id 
Group by month_id
order by 2 desc
;

-- best productline sales in November(monthid 11)

Select  month_id,convert(int,SUM(sales)) as Total_Sales, productline
From sales_data
where year_id = 2003 and month_id = 11
Group by month_id, productline
order by 2 desc
;

-- best customer

DROP TABLE IF EXISTS #rfm
;with rfm as 
(
select customername, 
	   max(orderdate) as last_order_date,
	   count(ordernumber) as Frequency,
	   convert(int,sum(sales)) as Monetaryvalue,
	   convert(int,avg(sales)) as AvgMonetaryvalue,
	   (select max(orderdate) from sales_data) as max_order_date,
	   datediff(DD,max(orderdate), (select max(orderdate) from sales_data)) as Recency 
from sales_data
group by customername
),
rfm_calc as 
(
	select r.*,
		NTILE(4) over (order by Recency desc) as rfm_recency,
		NTILE(4) over (order by Frequency) as rfm_frequency,
		NTILE(4) over (order by Monetaryvalue) as rfm_monetary
	from rfm r
)
select c.*,rfm_recency+rfm_frequency+rfm_monetary as rfm_cell,
	   cast(rfm_recency as varchar)+cast(rfm_frequency as varchar)+ cast(rfm_monetary as varchar) rfm_cell_string
into #rfm
from rfm_calc c

select customername, rfm_recency, rfm_frequency, rfm_monetary,
	case 
		when rfm_cell_string in (111, 112 , 121, 122, 123, 132, 211, 212, 114, 141) then 'lost_customers'  --lost customers
		when rfm_cell_string in (133, 134, 143, 244, 334, 343, 344, 144) then 'slipping away, cannot lose' -- (Big spenders who haven’t purchased lately) slipping away
		when rfm_cell_string in (311, 411, 331) then 'new customers'
		when rfm_cell_string in (222, 223, 233, 322) then 'potential churners'
		when rfm_cell_string in (323, 333,321, 422, 332, 432) then 'active' --(Customers who buy often & recently, but at low price points)
		when rfm_cell_string in (433, 434, 443, 444) then 'loyal'
	end rfm_segment	
from #rfm

-- products most often sold together
select distinct ordernumber, STUFF(
	(select ',' + productcode
	from sales_data p
	where ordernumber in
		(
		select ordernumber
		from (
			select ordernumber, count(*) rn
			from sales_data
			where status = 'shipped'
			group by ordernumber
			) m
		where rn = 2
		)
		and p.ordernumber = s.ordernumber
		for xml path ('')),1,1,'') Product_code
from sales_data s
order by 2 desc