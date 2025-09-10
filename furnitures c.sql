CREATE table furniture_s
LIKE furniture_sales;

insert into furniture_s
select *
from furniture_sales;

SELECT *
FROM furniture_s;

--- REMOVNG DUPLICATES
SELECT *, row_number() OVER(partition by Row_ID, order_ID, Order_Date, Ship_Date, Ship_Mode, Customer_ID, Customer_Name, Segment, Country, City, State, Postal_code) AS row_num
FROM furniture_s
;

with row_numb as
(SELECT *, row_number() OVER(partition by Row_ID, order_ID, Order_Date, Ship_Date, Ship_Mode, 
Customer_ID, Customer_Name, Segment, Country, City, State, Postal_code) AS row_num
FROM furniture_s
)
select *
from row_numb
where row_num > 1;

--- STANDARDIZING DAT
SELECT distinct Ship_mode
from furniture_s;

SELECT distinct Customer_Name
from furniture_s;

SELECT distinct segment
from furniture_s;

SELECT distinct Country
from furniture_s;

SELECT distinct City
from furniture_s;

SELECT distinct state
from furniture_s;

SELECT distinct Region
from furniture_s;

SELECT distinct Category
from furniture_s;

SELECT distinct Sub_Category
from furniture_s;

SELECT distinct Product_Name
from furniture_s;

select Order_Date
FROM furniture_s;

select Order_Date, 
str_to_date(Order_Date, '%m/%d/%Y')
FROM furniture_s;

update furniture_s
set Order_Date = str_to_date(Order_Date, '%m/%d/%Y');

alter table furniture_s
modify column Order_Date date;

select Ship_Date, 
str_to_date(Ship_Date, '%m/%d/%Y')
FROM furniture_s;

update furniture_s
set Ship_Date = str_to_date(Ship_Date, '%m/%d/%Y');

alter table furniture_s
modify column Ship_Date date;

--- DELETING COLN NOT NEEDED
alter table furniture_s
drop column Postal_Code;

alter table furniture_s
drop column Row_ID;

--- EXPLORATORY DATA


--- TOTAL SALES BY YEAR
select year(Order_Date) year, sum(Sales) as sales
from furniture_s
group by year
order by sales desc;

--- MONTHLY SALES TREND
select substring(Order_Date, 6, 2) months, sum(Sales) as sales 
from furniture_s
group by months
order by months;

--- TOTAL SALES BY CATEGORY
Select Sub_Category, sum(Sales) as sales
from furniture_s
group by Sub_Category
order by sales desc;

--- ORDER SHIPMENT DURATION
SELECT count(Order_ID) as order_id, datediff(Ship_Date, Order_Date) as duration
from furniture_s
group by duration
order by duration;

--- ORDER SHIPPING  MODE
SELECT Ship_Mode, count(Order_ID) as order_id
from furniture_s
group by Ship_Mode
order by order_id desc;

--- SALES BY CITY (TOP 5)
WITH CTE_EXAMPLE AS
(select city, sum(Sales) as sales
from furniture_s
group by city
order by sales desc),
ranking as
(SELECT *, dense_rank() over(order by sales desc) as ranks
FROM CTE_EXAMPLE)
select *
from ranking
where ranks <=5;

--- SALES BY STATE
WITH CTE_EXAMPLE AS
(select state, sum(Sales) as sales
from furniture_s
group by state
order by sales desc),
ranking as
(SELECT *, dense_rank() over(order by sales desc) as ranks
FROM CTE_EXAMPLE)
select *
from ranking;

--- TOTAL SALES EVERY YEAR ROLLING SUM
with cte_example as
(select year(Order_Date) year, sum(Sales) as sales 
from furniture_s
group by year
)
select year, sales, sum(sales) over(order by sales desc) as yearly_sales
from cte_example;


--- CUSTOMER WITH THE HIGHEST ORDER EVERY YEAR BY RANKS
with rolling_data as
(select Customer_Name, substring(Order_Date, 1, 4) years, sum(Sales) as sales 
from furniture_s
group by Customer_Name, years
order by years),
Ranking as
(select *, dense_rank() over(partition  by years order by sales desc) ranking
from rolling_data)
select *
from Ranking 
where ranking <=5;

--- WHICH REGION GENERATED MORE SALES BY RANKS EVERY YEAR
with rolling_data as
(select Region, substring(Order_Date, 1, 4) years, sum(Sales) as sales 
from furniture_s
group by Region, years
order by years),
Ranking as
(select *, dense_rank() over(partition  by years order by sales desc) ranking
from rolling_data)
select *
from Ranking 
where ranking <=4;

--- SALES PER YEAR AND PER SEGMENT
select year(Order_Date) as years, segment, sum(Sales) as sales
from furniture_s
group by segment, years
order by years;

--- SALES PER YEAR AND PER SEGMENT AND ALSO SHOWING THE ROLLING DATA
with cte_example as
(select year(Order_Date) as years, segment, sum(Sales) as sales
from furniture_s
group by years, segment
)
select years, segment, sales, sum(sales) over(partition by years order by sales desc) as rolling_data
from cte_example
order by years;








