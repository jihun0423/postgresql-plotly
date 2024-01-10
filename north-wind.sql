select * from nw.categories;

select * from nw.customers;

select * from nw.employees limit 3;

select * from order_items;

select * from orders;

select * from orders join order_items  using (order_id);
 
select date_trunc('day',order_date)::date date, sum(amount) sum, count(distinct nw.orders.order_id),
sum(amount)/count(distinct nw.orders.order_id) avg
from nw.orders join nw.order_items using (order_id)
group by date_trunc('day',order_date)::date 
order by 1;

with 
temp01 as (
select category_name, date_trunc('month',order_date)::date yyyymm, d.category_id, 
sum(amount) as sum_amt, count(distinct a.order_id)
from nw.orders a 
join nw.order_items b on a.order_id = b.order_id
join nw.products c on b.product_id = c.product_id 
join nw.categories d on c.category_id = d.category_id 
group by date_trunc('month',order_date)::date,d.category_id  
order by yyyymm
)
select *,
sum(sum_amt) over (partition by yyyymm) month_total,
sum_amt/sum(sum_amt) over (partition by yyyymm) ratio
from temp01;


select to_char(date_trunc('month',order_date),'yyyymm')
from nw.orders 

with temp02 as(
select d.category_name,c.product_name, sum(amount) product_sum, count(distinct a.order_id)
from nw.orders a
join nw.order_items b on a.order_id = b.order_id 
join nw.products c on b.product_id = c.product_id 
join nw.categories d on c.category_id = d.category_id 
group by d.category_name,c.product_name 
)
select *,sum(product_sum) over(partition by category_name) category_sum,
round(product_sum/sum(product_sum) over(partition by category_name),3) product_ratio,
row_number() over(partition by category_name order by product_sum desc) ranking
from temp02
order by category_name,ranking

select a.ship_country country, d.category_name, sum(b.amount) sum_amount, count(distinct a.order_id) num_people
from nw.orders a
join nw.order_items b on a.order_id = b.order_id 
join nw.products c on b.product_id = c.product_id 
join nw.categories d on c.category_id = d.category_id 
group by a.ship_country,d.category_name
order by a.ship_country, d.category_name

with temp03
as(
select date_trunc('month',a.order_date)::date yyyymm, 
sum(b.amount) monthly_sum, count(distinct a.order_id) monthly_count
from nw.orders a
join nw.order_items b on a.order_id = b.order_id 
group by date_trunc('month',a.order_date)::date
)
select *,left(to_char(yyyymm,'yyyymm'),4),sum(monthly_sum) over (partition by date_trunc('year',yyyymm)::date order by yyyymm ) cumsum
from temp03


with temp04
as(
select date_trunc('day',a.order_date)::date date,sum(amount) sum_amount
from nw.orders a
join nw.order_items b on a.order_id = b.order_id 
group by date_trunc('day',a.order_date)::date
order by date
)
select *,avg(sum_amount) over(order by date rows between 4 preceding and current row) ma5
from temp04

select date_trunc('month',a.order_date)::date date,sum(amount) sum_amount
from nw.orders a
join nw.order_items b on a.order_id = b.order_id 
group by date_trunc('month',a.order_date)::date
order by date


with temp05
as(
select date_trunc('day',a.order_date)::date date,sum(amount) sum_amount,row_number() over(order by date_trunc('day',a.order_date)::date)
from nw.orders a
join nw.order_items b on a.order_id = b.order_id 
group by date_trunc('day',a.order_date)::date
order by date
)
select *, avg(b.sum_amount) over(partition by a.row_number)
from temp05 a
join temp05 b on a.row_number between b.row_number and b.row_number+4 


with 
temp_01 as (
select date_trunc('day', order_date)::date as d_day
	, sum(amount) as sum_amount
	, row_number() over (order by date_trunc('day', order_date)::date) as rnum
from nw.orders a
	join nw.order_items b on a.order_id = b.order_id
group by date_trunc('day', order_date)::date
),
temp_02 as (
select a.d_day, a.sum_amount, a.rnum, b.d_day as d_day_back, b.sum_amount as sum_amount_back, b.rnum as rnum_back 
from temp_01 a
	join temp_01 b on a.rnum between b.rnum and b.rnum + 4 
)
select d_day
	, avg(sum_amount_back) as m_avg_5days
	, sum(sum_amount_back)/5 as m_avg_5days_01
	, sum(case when rnum - rnum_back = 4 then 0.5 * sum_amount_back
	           when rnum - rnum_back in (3, 2, 1) then sum_amount_back
	           when rnum - rnum_back = 0 then 1.5 * sum_amount_back 
	      end) as m_weighted_sum
	, sum(case when rnum - rnum_back = 4 then 0.5 * sum_amount_back
		   when rnum - rnum_back in (3, 2, 1) then sum_amount_back
		   when rnum - rnum_back = 0 then 1.5 * sum_amount_back 
	      end) / 5 as m_w_avg_sum
        -- 5건이 안되는 초기 데이터는 삭제하기 위해서임.  
	, count(*) as cnt
from temp_02
group by d_day
having count(*) = 5
order by d_day
;

select to_char(date_trunc('year',a.order_date),'yyyy')
from nw.orders a

with temp06
as(
select date_trunc('month',a.order_date)::date date, sum(b.amount)
from nw.orders a
join nw.order_items b on a.order_id = b.order_id 
group by date_trunc('month',a.order_date)::date
order by date
)
select a.date prev_month, a.sum prev_amount, b.date curr_month, b.sum curr_amount,
b.sum-a.sum diff_amount, ((b.sum-a.sum)/a.sum)*100 growth_percent
from temp06 a
join temp06 b on to_char(date_trunc('year',a.date),'yyyy') < to_char(date_trunc('year',b.date),'yyyy')
and to_char(date_trunc('month',a.date),'mm') = to_char(date_trunc('month',b.date),'mm') 
order by a.date


with temp10 as(
select a.country,e.category_name,sum(c.amount) sum_amount
from nw.customers a
join nw.orders b on a.customer_id = b.customer_id 
join nw.order_items c on b.order_id = c.order_id 
join nw.products d on d.product_id = c.product_id 
join nw.categories e on d.category_id = e.category_id 
group by a.country,e.category_name
order by a.country
)
select *,sum_amount/sum(sum_amount) over(partition by country) ratio from temp10


with temp07 as
(
select date_trunc('month',a.order_date)::date,d.category_name,
sum(b.amount),count(distinct a.order_id)
from nw.orders a
join nw.order_items b on a.order_id = b.order_id 
join nw.products c on b.product_id = c.product_id 
join nw.categories d on c.category_id = d.category_id 
group by d.category_name,date_trunc('month',a.order_date)::date
)
select *,first_value(sum) over(partition by category_name) from temp07
where date_trunc < to_date('1997-06-30','yyyy-mm-dd')


with temp08_01 as
(
select date_trunc('month',a.order_date)::date date, sum(amount) monthly_sum,
row_number() over(order by date_trunc('month',a.order_date)::date) r_num
from nw.orders a
join nw.order_items b on a.order_id = b.order_id 
group by date_trunc('month',a.order_date)::date
order by date
),
temp08_02 as(
select date, monthly_sum,sum(monthly_sum) over(rows between 12 preceding and current row) ma12_sum
from temp08_01
)
select a.date,a.monthly_sum, sum(b.monthly_sum) over(order by a.date) accum_sum,b.ma12_sum
from temp08_01 a
join temp08_02 b on a.date = b.date
where a.r_num>10


with temp11_01 as(
select a.customer_id,a.order_date,
lag(a.order_date) over(partition by a.customer_id order by a.order_date) prior_date
from nw.orders a 
join nw.order_items b on a.order_id = b.order_id 
join nw.customers c on a.customer_id = c.customer_id 
join nw.products d on b.product_id = d.product_id 
join nw.categories e on d.category_id = e.category_id 
group by a.customer_id,a.order_date
order by a.customer_id,a.order_date
),
temp11_02 as(
select *, floor((order_date-prior_date)/10.0)*10 days_since_prior
from temp11_01
)
select *
from temp11_02
join nw.customers c2 on temp11_02.customer_id = c2.customer_id 

with temp12 as(
select a.customer_id,date_trunc('month',a.order_date)::DATE monthly , 
count(distinct a.order_id) num_orders,sum(b.amount) monthly_sum
from nw.orders a
join nw.order_items b on a.order_id = b.order_id 
join nw.products c on b.product_id = c.product_id 
join nw.categories d on c.category_id = d.category_id 
group by a.customer_id,date_trunc('month',a.order_date)::DATE
order by a.customer_id,date_trunc('month',a.order_date)::DATE
)
select monthly, avg(num_orders), max(num_orders), min(num_orders),avg(monthly_sum)
from temp12
group by monthly 
order by monthly

with temp12_02 as(
select date_trunc('month',a.order_date)::DATE monthly , 
count(distinct a.order_id) num_orders,count(distinct a.customer_id) num_customers,sum(b.amount) monthly_sum
from nw.orders a
join nw.order_items b on a.order_id = b.order_id 
join nw.products c on b.product_id = c.product_id 
join nw.categories d on c.category_id = d.category_id 
group by date_trunc('month',a.order_date)::DATE
order by date_trunc('month',a.order_date)::DATE
)
select *, num_orders/cast(num_customers as double precision)
from temp12_02
order by monthly
