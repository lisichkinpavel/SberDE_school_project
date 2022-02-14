--- Пример запросов для формирования отчета


--- 1) Совершение операции при просроченном паспорте.

select ft.trans_date fraud_dt, 
	   cl.passport_num passport,
	   cl.last_name || ' ' || cl.first_name || ' ' || cl.patronymic fio,
	   cl.phone,
	   '1' fraud_type,
	   now() report_dt
from project.fact_transactions ft 
left join 
	(select * from project.dim_cards_hist where date(end_dt) = '9999-12-31') c
	 on ft.card_num = c.card_num
left join
	(select * from project.dim_accounts_hist where date(end_dt) = '9999-12-31') a
	on c.account_num = a.account_num
left join
	(select * from project.dim_clients_hist where date(end_dt) = '9999-12-31') cl
	on cl.client_id = a.client
where date(ft.trans_date) = '2020-05-03'
	  and  date(ft.trans_date) > cl.passport_valid_to;
	 
	 

--- 2) Совершение операции при недействующем договоре.

select ft.trans_date fraud_dt,
	   cl.passport_num passport,
	   cl.last_name || ' ' || cl.first_name || ' ' || cl.patronymic fio,
	   cl.phone,
	   '2' fraud_type,
	   now() report_dt
from project.fact_transactions ft 
left join 
	(select * from project.dim_cards_hist where date(end_dt) = '9999-12-31') c
	 on ft.card_num = c.card_num
left join
	(select * from project.dim_accounts_hist where date(end_dt) = '9999-12-31') a
	on c.account_num = a.account_num
left join
	(select * from project.dim_clients_hist where date(end_dt) = '9999-12-31') cl
	on cl.client_id = a.client
where date(ft.trans_date) = '2020-05-03'
	  and  date(ft.trans_date) > a.valid_to;
	 
	 
	 
--- 3) Совершение операции в разных городах в течение 1 часа.

	 
with cte as ( 
    select 
    c.account_num, 
    ft.trans_date, 
    t.terminal_city, 
    lead(t.terminal_city) over (partition by c.account_num order by ft.trans_date asc) next_trans_city, 
    lead(ft.trans_date) over (partition by c.account_num order by ft.trans_date asc) next_trans_date, 
    extract (epoch from (lead(ft.trans_date) over (partition by c.account_num order by ft.trans_date asc) -  trans_date)) /3600 delta_in_hours 
from project.fact_transactions ft 
left join 
    (select * from project.dim_terminals_hist where date(end_dt) = '9999-12-31') t 
    on ft.terminal = t.terminal_id 
left join 
    (select * from project.dim_cards_hist where date(end_dt) = '9999-12-31') c 
    on ft.card_num = c.card_num
where date(ft.trans_date) = '2020-05-03')
select cte.next_trans_date fraud_dt,
    cl.passport_num passport,
    cl.last_name || ' ' || cl.first_name || ' ' || cl.patronymic fio,
    cl.phone,
    '3' fraud_type,
    now() report_dt
from cte 
left join
    (select * from project.dim_accounts_hist where date(end_dt) = '9999-12-31') a
    on cte.account_num = a.account_num
left join
    (select * from project.dim_clients_hist where date(end_dt) = '9999-12-31') cl
    on cl.client_id = a.client
where next_trans_city is not null 
    and terminal_city <> next_trans_city 
    and delta_in_hours < 1 ;



----- 4) Попытка подбора сумм. В течение 20 минут проходит более 3х операций со следующим
--		 шаблоном – каждая последующая меньше предыдущей, при этом отклонены все, кроме последней. 
--		 Последняя операция (успешная) в такой цепочке считается мошеннической.

with cte as(
	select 
	ft.trans_id,
	ft.trans_date,
	c.card_num,
	ft.oper_type,
	ft.oper_result,
	ft.amt,
	count(*) over(partition by c.card_num) trans_count 
from project.fact_transactions ft 
left join 
	(select * from project.dim_cards_hist where date(end_dt) = '9999-12-31') c
on ft.card_num = c.card_num
where date(ft.trans_date) = '2020-05-03'
order by c.card_num, ft.trans_date
),
cte2 as(
select * from cte
where cte.trans_count > 3),
cte3 as
(select card_num,amt,
trans_date,
oper_result,
lag(amt,1) over(partition by card_num order by trans_date) amt1,
lag(oper_result,1) over(partition by card_num order by trans_date) status_prev1,
lag(trans_date,1) over(partition by card_num order by trans_date) trans_date1,
lag(amt,2) over(partition by card_num order by trans_date) amt2,
lag(oper_result,2) over(partition by card_num order by trans_date) status_prev2,
lag(trans_date,2) over(partition by card_num order by trans_date) trans_date2,
lag(amt,3) over(partition by card_num order by trans_date) amt3,
lag(oper_result,3) over(partition by card_num order by trans_date) status_prev3,
lag(trans_date,3) over(partition by card_num order by trans_date) trans_date3
from cte2)

select cte3.trans_date fraud_dt,
    cl.passport_num passport,
    cl.last_name || ' ' || cl.first_name || ' ' || cl.patronymic fio,
    cl.phone,
    '4' fraud_type,
    now() report_dt
from cte3 
left join
    (select * from project.dim_cards_hist where date(end_dt) = '9999-12-31') c
    on cte3.card_num = c.card_num
left join
    (select * from project.dim_accounts_hist where date(end_dt) = '9999-12-31') a
    on c.account_num = a.account_num
left join
    (select * from project.dim_clients_hist where date(end_dt) = '9999-12-31') cl
    on cl.client_id = a.client

where oper_result = 'Успешно' 
	  and status_prev1 = 'Отказ'
	  and status_prev2 = 'Отказ'
	  and status_prev3 = 'Отказ'
	  and amt < amt1 and amt1 < amt2 and amt2 < amt3
	  and extract (epoch from(trans_date - trans_date3)) / 60 <= 20
;