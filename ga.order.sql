select * from ga.ga_sess

select * from ga.ga_sess_hits

select * from ga.ga_users 

select * from ga.orders 

select * from ga.order_items 

select * from ga.products 


select d.prod_cat,sum(b.prod_price*b.prod_quantity) sum_profit,count(distinct a.order_id) num_orders,
sum(b.prod_price*b.prod_quantity) / count(distinct a.order_id) avg_profit_per_orders,
avg(b.prod_price*b.prod_quantity) avg_profit
from ga.orders a
join ga.order_items b on a.order_id = b.order_id 
join ga.ga_users c on a.user_id = c.user_id 
join ga.products d on b.product_id = d.product_id 
group by d.prod_cat


select * from ga.products 



select c.prod_cat
from ga.orders a
join ga.order_items b on a.order_id = b.order_id 
join ga.products c on b.product_id = c.product_id 
group by c.prod_cat


with temp15_01 as(
select *
from ga.orders a
join ga.order_items b on a.order_id = b.order_id 
),
temp15_02 as(
select a.product_id prod01,b.product_id prod02 from temp15_01 a
join temp15_01 b on a.user_id = b.user_id where a.product_id != b.product_id
)
select prod01, prod02, count(*) from temp15_02
group by prod01, prod02
order by count desc

with 
-- user_id는 order_items에 없으므로 order_items와 orders를 조인하여 user_id 추출. 
temp_00 as (
select b.user_id,a.order_id, a.product_id
from order_items a
	join orders b on a.order_id = b.order_id
), -- temp_00을 user_id로 셀프 조인하면 M:M 조인되면서 개별 user_id별 주문 상품별로 연관된 주문 상품 집합을 생성
temp_01 as
(
select a.user_id, a.product_id as prod_01, b.product_id as prod_02 
from temp_00 a
	join temp_00 b on a.user_id = b.user_id
where a.product_id != b.product_id
), 
-- prod_01 + prod_02 레벨로 group by 건수를 추출. 
temp_02 as (
select prod_01, prod_02, count(*) as cnt
from temp_01 
group by prod_01, prod_02
)
select * from temp_02
, 
temp_03 as (
select prod_01, prod_02, cnt
	-- prod_01별로 가장 많은 건수를 가지는 prod_02를 찾기 위해 cnt가 높은 순으로 순위추출. 
    , row_number() over (partition by prod_01 order by cnt desc) as rnum
from temp_02
)
select prod_01, prod_02, cnt, rnum
from temp_03
where rnum = 1
;

with temp16_01 as(
select user_id, max(order_time),
to_date('20161101','yyyymmdd')-max(date_trunc('day',order_time)::date) recency, 
count(distinct a.order_id) frequency, 
sum(b.prod_price * b.prod_quantity) monetary,
count(distinct a.order_id) num_orders,
sum(b.prod_price * b.prod_quantity)/count(distinct a.order_id) average_order_per_user
from ga.orders a
join ga.order_items b on a.order_id = b.order_id 
join ga.products c on b.product_id = c.product_id 
group by user_id
),
temp16_02_01 as(
select user_id, a.order_id,
sum(b.prod_price * b.prod_quantity) order_sum
from ga.orders a
join ga.order_items b on a.order_id = b.order_id 
join ga.products c on b.product_id = c.product_id
group by user_id, a.order_id
),
temp16_02 as(
select user_id, max(order_sum) max_order_per_user
from temp16_02_01
group by user_id
),
temp16_03 as(
select * from temp16_01 join temp16_02 using (user_id)
),
temp16_04 as(
select user_id,order_time
from ga.orders a
join ga.order_items b on a.order_id = b.order_id 
join ga.products c on b.product_id = c.product_id
group by user_id,order_time
order by user_id,order_time
),
temp16_05 as(
select *,lag(order_time) over(partition by user_id) prev_order_time, 
to_char(order_time - lag(order_time) over(partition by user_id),'dd')::float8 days_between_orders
from temp16_04
),
temp16_06 as(
select user_id,coalesce(avg(days_between_orders),100) avg_between_orders from temp16_05 group by user_id 
)
select distinct * from temp16_03 join temp16_06 using(user_id)
	