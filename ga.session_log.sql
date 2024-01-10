-- dau,wau,mau version 1
select :current_day, count(distinct user_id) as dau
from ga.ga_sess
where visit_stime >= (:current_day - interval '7 days') and visit_stime < :current_day

create table if not exists daily_acquisitions
(d_day date,
dau integer,
wau integer,
mau integer
);

insert into daily_acquisitions 
select :current_date,
(select count(distinct user_id) as dau from ga.ga_sess 
where visit_stime >= (:current_day - interval '1 days') and visit_stime < :current_day),
(select count(distinct user_id) as wau from ga.ga_sess 
where visit_stime >= (:current_day - interval '7 days') and visit_stime < :current_day),
(select count(distinct user_id) as mau from ga.ga_sess 
where visit_stime >= (:current_day - interval '30 days') and visit_stime < :current_day)

select * from daily_acquisitions 

with temp1 as (
select visit_date from ga.ga_sess group by visit_date
)
select to_date(visit_date,'yyyymmdd') from temp1

select to_date(visit_date,'yyyymmdd') visit_date from ga.ga_sess group by to_date(visit_date,'yyyymmdd')

do $$
declare
f record;
begin
for f in select to_date(visit_date,'yyyymmdd') visit_date from ga.ga_sess group by to_date(visit_date,'yyyymmdd')
order by to_date(visit_date,'yyyymmdd')
loop 
raise notice 'visit_date: %', f.visit_date;
end loop;
end;
$$;


do $$
declare
f record;
begin
for f in select to_date(visit_date,'yyyymmdd') visit_date from ga.ga_sess group by to_date(visit_date,'yyyymmdd')
order by to_date(visit_date,'yyyymmdd')
loop 
raise notice 'visit_date: %', f.visit_date;
insert into ga.daily_acquisitions 
select f.visit_date,
(select count(distinct user_id) as dau from ga.ga_sess 
where visit_stime >= (f.visit_date - interval '1 days') and visit_stime < f.visit_date),
(select count(distinct user_id) as wau from ga.ga_sess 
where visit_stime >= (f.visit_date - interval '7 days') and visit_stime < f.visit_date),
(select count(distinct user_id) as mau from ga.ga_sess 
where visit_stime >= (f.visit_date - interval '30 days') and visit_stime < f.visit_date);
end loop;
end;
$$;
commit;

select * from daily_acquisitions 


-- dau, wau, mau version 2

create table daily_acquisitions2(
curr_date date,
dau integer,
wau integer,
mau integer)

create table dau as
with temp_20_01 as(
select generate_series('2016-08-02'::date, '2016-11-01'::date, '1 day'::interval)::date curr_date
)
select curr_date, count(distinct user_id) dau
from temp_20_01 a cross join ga.ga_sess b 
where b.visit_stime >= (a.curr_date - interval '1 days') and a.curr_date > (b.visit_stime)
group by curr_date

create table wau as
with temp_20_01 as(
select generate_series('2016-08-02'::date, '2016-11-01'::date, '1 day'::interval)::date curr_date
)
select curr_date, count(distinct user_id) dau
from temp_20_01 a cross join ga.ga_sess b 
where b.visit_stime >= (a.curr_date - interval '7 days') and a.curr_date > (b.visit_stime)
group by curr_date

create table mau as
with temp_20_01 as(
select generate_series('2016-08-02'::date, '2016-11-01'::date, '1 day'::interval)::date curr_date
)
select curr_date, count(distinct user_id) dau
from temp_20_01 a cross join ga.ga_sess b 
where b.visit_stime >= (a.curr_date - interval '30 days') and a.curr_date > (b.visit_stime)
group by curr_date

insert into daily_acquisitions2 
select * 
from dau a join wau b using (curr_date) join mau c using (curr_date)

select *,100.0*dau/mau stickiness from daily_acquisitions2  

with temp21_01 as(
select user_id, date_trunc('month',visit_stime)::date as month,count(*) monthly_cnt 
from ga.ga_sess group by user_id, date_trunc('month',visit_stime)
),
temp21_02 as(
select month, 
	case when monthly_cnt <= 1 then '0~1'
	when monthly_cnt between 2 and 3 then '2~3'
	when monthly_cnt between 4 and 8 then '4~8'
	when monthly_cnt between 9 and 14 then '9~14'
	when monthly_cnt between 15 and 25 then '15~25'	
	when monthly_cnt >= 26 then '26 over' end as cnt_rank,
	count(*) cnt
from temp21_01
group by month, 
	case when monthly_cnt <= 1 then '0~1'
	when monthly_cnt between 2 and 3 then '2~3'
	when monthly_cnt between 4 and 8 then '4~8'
	when monthly_cnt between 9 and 14 then '9~14'
	when monthly_cnt between 15 and 25 then '15~25'	
	when monthly_cnt >= 26 then '26 over' end
	order by 1,2
)
select month,
sum(case when cnt_rank = '0~1' then cnt else 0 end) as "0~1",
sum(case when cnt_rank = '2~3' then cnt else 0 end) as "2~3",
sum(case when cnt_rank = '4~8' then cnt else 0 end) as "4~8",
sum(case when cnt_rank = '9~14' then cnt else 0 end) as "9~14",
sum(case when cnt_rank = '15~25' then cnt else 0 end) as "15~25",
sum(case when cnt_rank = '26 over' then cnt else 0 end) as "26 over"
from temp21_02
group by month 

with temp22_01 as(
select user_id,visit_stime as sess_time,
row_number() over (partition by user_id) r_num
from ga.ga_sess
group by user_id,visit_stime
order by user_id,visit_stime
),
temp22_02 as(
select user_id, sess_time, lag(sess_time) over (partition by user_id) first_day,
sess_time - lag(sess_time) over (partition by user_id) time_between
from temp22_01
where r_num <= 2
)
select justify_interval(avg(time_between)) avg_time ,max(time_between) max_time,
min(time_between) min_time, 
percentile_cont(0.25) within group (order by time_between asc) percentile_1,
percentile_cont(0.5) within group (order by time_between asc) percentile_2,
percentile_cont(0.75) within group (order by time_between asc) percentile_3,
percentile_cont(1.0) within group (order by time_between asc) percentile_4
from temp22_02 where time_between is not null and time_between::interval > interval '0 second'


with temp23_01 as(
select a.sess_id, a.user_id, a.visit_stime, b.create_time,
case when b.create_time >= (:curr_time - interval '30 days') and 
b.create_time < :curr_time
then 1 else 0 end as is_new_user
from ga.ga_sess a join ga.ga_users b on a.user_id = b.user_id
)
select count(distinct user_id) user_cnt, 
count(distinct case when is_new_user = 1 then user_id end) as new_user_cnt,
count(distinct case when is_new_user = 0 then user_id end) as repeat_user_cnt,
count(*) as sess_cnt
from temp23_01



with temp24_01 as(
select generate_series('2016-08-01'::date,'2016-11-01'::date,'1 day'::interval) days_list
),
temp24_02 as(
select a.user_id, a.sess_id, a.visit_stime, b.create_time 
from ga.ga_sess a join ga.ga_users b on a.user_id = b.user_id 
),
temp24_03 as(
select *,
case when create_time >= (days_list - interval '30 days') 
and create_time < days_list then 1 else 0 end as is_new_user
from temp24_01 cross join temp24_02
where visit_stime < days_list and visit_stime >= (days_list - interval '30 days') 
)
select days_list, count(distinct user_id), count(distinct case when is_new_user = 1 then user_id end) as new_user_cnt,
count(distinct case when is_new_user = 0 then user_id end) as repeat_user_cnt,
count(*) sess_cnt
from temp24_03
group by days_list

