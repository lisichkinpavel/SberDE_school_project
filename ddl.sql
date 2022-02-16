
-- create schema if not exists project;

-- creating stg tables

drop table if exists project.stg_terminals cascade;
create table project.stg_terminals
(
 terminal_id     varchar   not null,
 terminal_type   varchar   not null,
 terminal_city   varchar   not null,
 terminal_adress varchar   not null,
 start_dt 	     timestamp not null
)
with (appendonly=true)
distributed by (terminal_id);

drop table if exists project.stg_clients cascade;
create table project.stg_clients
(
 client_id         varchar   not null,
 last_name         varchar   not null,
 first_name        varchar   not null,
 patronymic        varchar   not null,
 date_of_birth     date      not null,
 passport_num      varchar   not null,
 passport_valid_to date      not null,
 phone             varchar   not null,
 start_dt 	       timestamp not null
)
with (appendonly=true)
distributed by (client_id);


drop table if exists project.stg_accounts cascade;
create table project.stg_accounts
(
 account_num varchar   not null,
 valid_to    date      not null,
 client      varchar   not null,
 start_dt    timestamp not null
)
with (appendonly=true)
distributed by (account_num);

drop table if exists project.stg_cards cascade;
create table project.stg_cards
(
 card_num    varchar   not null,
 account_num varchar   not null,
 start_dt 	 timestamp not null
)
with (appendonly=true)
distributed by (card_num);

-----------------------------------------------------------------------

-- creating fact table
drop table if exists project.fact_transactions cascade;
create table project.fact_transactions
(
 trans_id    varchar   not null,
 trans_date  timestamp not null,
 card_num    varchar   not null,
 oper_type   varchar   not null,
 amt         decimal   not null,
 oper_result varchar   not null,
 terminal    varchar   not null
)
with (appendonly=true)
distributed by (trans_id)
partition by range (trans_date)
( 	partition p1 start (date '2020-05-01') inclusive,
	partition p2 start (date '2020-05-02') inclusive,
	partition p3 start (date '2020-05-03') inclusive
   end (date '2020-05-04') exclusive);
  
--alter table project.fact_transactions drop partition p1;
--alter table project.fact_transactions drop partition p2;
  
-----------------------------------------------------------------------

-- creating dim tables

drop table if exists project.dim_terminals_hist cascade;
create table project.dim_terminals_hist
(
 terminal_id     varchar   not null,
 terminal_type   varchar   not null,
 terminal_city   varchar   not null,
 terminal_adress varchar   not null,
 start_dt 	     timestamp not null,
 end_dt 	     timestamp not null
)
with (appendonly=true)
distributed by (terminal_id);

drop table if exists project.dim_clients_hist cascade;
create table project.dim_clients_hist
(
 client_id         varchar   not null,
 last_name         varchar   not null,
 first_name        varchar   not null,
 patronymic        varchar   not null,
 date_of_birth     date      not null,
 passport_num      varchar   not null,
 passport_valid_to date      not null,
 phone             varchar   not null,
 start_dt 	       timestamp not null,
 end_dt 	       timestamp not null
)
with (appendonly=true)
distributed by (client_id);


drop table if exists project.dim_accounts_hist cascade;
create table project.dim_accounts_hist
(
 account_num varchar   not null,
 valid_to    date      not null,
 client      varchar   not null,
 start_dt    timestamp not null,
 end_dt 	 timestamp not null
)
with (appendonly=true)
distributed by (account_num);

drop table if exists project.dim_cards_hist cascade;
create table project.dim_cards_hist
(
 card_num    varchar   not null,
 account_num varchar   not null,
 start_dt 	 timestamp not null,
 end_dt 	 timestamp not null
)
with (appendonly=true)
distributed by (card_num);

-----------------------------------------------------------------------
-- create views

-- view для таблицы terminals
create or replace view project.terminals_update_view as
select
    stg.*
from
    project.stg_terminals stg
left join project.dim_terminals_hist dth
on
	dth.terminal_id = stg.terminal_id
	and dth.terminal_type = stg.terminal_type
    and dth.terminal_adress = stg.terminal_adress
    and dth.terminal_city = stg.terminal_city
where
    dth.terminal_id is null;

-- view для таблицы cards
create or replace view project.cards_update_view as
select
    stg.*
from
    project.stg_cards stg
left join project.dim_cards_hist dth
on
	dth.account_num = stg.account_num
    and dth.card_num = stg.card_num
where
    dth.account_num is null;


-- view для таблицы account
create or replace view project.accounts_update_view as
select
    stg.*
from
    project.stg_accounts stg
left join project.dim_accounts_hist dth
on
	dth.account_num = stg.account_num
    and dth.valid_to = stg.valid_to 
    and dth.client = dth.client 
where
    dth.account_num is null;


-- view для таблицы account
create or replace view project.clients_update_view as
select
    stg.*
from
    project.stg_clients stg
left join project.dim_clients_hist dth
on
	dth.client_id = stg.client_id
    and dth.last_name = stg.last_name 
    and dth.first_name = stg.first_name 
    and dth.patronymic  = stg.patronymic 
    and dth.date_of_birth = stg.date_of_birth 
    and dth.passport_num  = stg.passport_num 
    and dth.passport_valid_to = stg.passport_valid_to   
    and dth.phone = dth.phone 
where
    dth.client_id is null;

   
-- report table

drop table if exists project.report cascade;
create table project.report
(
 fraud_dt   timestamp not null,
 passport   varchar   not null,
 fio   		varchar   not null,
 phone      varchar   not null,
 fraud_type varchar   not null,
 report_dt 	timestamp not null
)
with (appendonly=true)
distributed randomly;

