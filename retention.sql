-- retention
select count(distinct user_id), 
count(distinct case when last_date >= (:curr_date + interval '1 day') then user_id end) day_1_retention,
count(distinct case when last_date >= (:curr_date + interval '7 day') then user_id end) day_7_retention,
count(distinct case when last_date >= (:curr_date + interval '30 day') then user_id end) day_30_retention
from(
select *
from ga.ga_sess a join ga.ga_users b using (user_id)
where date_trunc('day',create_time)::date = :curr_date
) a join 
(
select distinct user_id,
last_value(visit_stime)
over (partition by user_id order by visit_stime rows between unbounded preceding and unbounded following) last_date
from ga.ga_sess 
) b using (user_id);


with temp_01 as(
select *,a.user_id users_id,coalesce(b.last_date,c.create_time) last_active_date
from ga.ga_sess a 
left outer join (select distinct user_id, 
last_value(visit_stime) over (partition by user_id order by visit_stime rows between unbounded preceding and unbounded following) last_date
from ga.ga_sess_hits join ga.ga_sess using(sess_id)
where hit_seq > 1) b on a.user_id = b.user_id
left outer join ga.ga_users c on a.user_id = c.user_id
where date_trunc('day',create_time)::date = :curr_date
)
select :curr_date create_date,count(distinct users_id), 
count(distinct case when last_date >= (:curr_date + interval '1 day') then users_id end) day_1_retention,
count(distinct case when last_date >= (:curr_date + interval '7 day') then users_id end) day_7_retention,
count(distinct case when last_date >= (:curr_date + interval '30 day') then users_id end) day_30_retention
from temp_01

with temp_01 as(
select *
from 
(select generate_series(to_date('2016-08-01','yyyy-mm-dd'),to_date('2016-10-01','yyyy-mm-dd'),interval '1 day') c_time) a
cross join (ga.ga_users b join ga.ga_sess c using (user_id)) 
where date_trunc('day',b.create_time) = c_time
)
select c_time, count(distinct user_id) created_count, 
count(distinct case when date_trunc('day',visit_stime) = (c_time + interval '1 day') then user_id end) day_1_retention,
count(distinct case when date_trunc('day',visit_stime) = (c_time + interval '2 days') then user_id end) day_2_retention,
count(distinct case when date_trunc('day',visit_stime) = (c_time + interval '3 days') then user_id end) day_3_retention,
count(distinct case when date_trunc('day',visit_stime) = (c_time + interval '4 days') then user_id end) day_4_retention,
count(distinct case when date_trunc('day',visit_stime) = (c_time + interval '5 days') then user_id end) day_5_retention,
count(distinct case when date_trunc('day',visit_stime) = (c_time + interval '6 days') then user_id end) day_6_retention,
count(distinct case when date_trunc('day',visit_stime) = (c_time + interval '7 days') then user_id end) day_7_retention,
count(distinct case when date_trunc('day',visit_stime) = (c_time + interval '30 days') then user_id end) month_retention
from temp_01
group by c_time


select create_time, count(distinct user_id),
count(distinct case when visit_stime = (create_time + interval '1 day') then user_id end) day_1_retention
from(
select user_id,date_trunc('day',create_time) create_time,date_trunc('day',visit_stime) visit_stime
from ga.ga_users a join ga.ga_sess b using (user_id)
group by user_id,date_trunc('day',create_time),date_trunc('day',visit_stime)
)
group by create_time

with temp_01 as(
select *
from (select distinct date_trunc('week',create_time) c_time
	  from ga.ga_users order by c_time) a cross join 
	(ga.ga_users join ga.ga_sess using (user_id)) b
where a.c_time = date_trunc('week',b.create_time)
)
select channel_grouping,c_time,count(distinct user_id),
count(distinct case when date_trunc('week',visit_stime) = (c_time + interval '1 week') then user_id end) w1_retention,
count(distinct case when date_trunc('week',visit_stime) = (c_time + interval '2 week') then user_id end) w2_retention,
count(distinct case when date_trunc('week',visit_stime) = (c_time + interval '3 week') then user_id end) w3_retention,
count(distinct case when date_trunc('week',visit_stime) = (c_time + interval '4 week') then user_id end) w4_retention,
count(distinct case when date_trunc('week',visit_stime) = (c_time + interval '5 week') then user_id end) w5_retention,
count(distinct case when date_trunc('week',visit_stime) = (c_time + interval '6 week') then user_id end) w6_retention,
count(distinct case when date_trunc('week',visit_stime) = (c_time + interval '7 week') then user_id end) w7_retention
from temp_01
group by channel_grouping,c_time

select * from ga.order_items

select date_trunc('day',visit_stime) sess_day,count(distinct a.sess_id) num_sess, 
count(distinct case when (action_type = '6') then a.sess_id end) num_sales
from ga.ga_sess a left outer join ga.ga_sess_hits b using(sess_id)
group by date_trunc('day',visit_stime)

with temp_01 as(
select order_id,user_id,sess_id, sum_revenue from ga.orders 
left outer join
(select order_id, sum(prod_revenue) sum_revenue from ga.orders join ga.order_items using(order_id)
group by order_id) 
using (order_id)
),
temp_02 as (
select date_trunc('day',visit_stime) sess_day,sum(sum_revenue) sum_revenue
from ga.ga_sess left outer join temp_01 using (sess_id)
group by date_trunc('day',visit_stime)
order by sess_day
),
temp_03 as (
select date_trunc('day',visit_stime) sess_day,count(distinct a.sess_id) num_sess, 
count(distinct case when (action_type = '6') then a.sess_id end) num_sales
from ga.ga_sess a left outer join ga.ga_sess_hits b using(sess_id)
group by date_trunc('day',visit_stime) 
)
select temp_03.*,100.0*num_sales/num_sess sales_cv_rate, sum_revenue, sum_revenue/num_sales revenue_per_perchase
from temp_03 join temp_02 using (sess_day)