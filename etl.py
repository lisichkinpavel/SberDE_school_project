#!/usr/bin/env python
# coding: utf-8


import io
import pandas as pd
import psycopg2
import os
import re


# Указываем путь к папке, в которую приходят xlsx файлы с транзакциями


stg_dir = '/Users/pavellisichkin/Desktop/stg/'


# Смотрим директорию. Получаем имя файла


filename = [file for file in os.listdir(stg_dir) if file.endswith('.xlsx')][0]


# Из имени файла возьмем дату, за которую будем выгружать данные


curr_date = re.findall(r'(\d{2})(\d{2})(\d{4})', filename)[0]
curr_date = f'{curr_date[2]}-{curr_date[1]}-{curr_date[0]}'


# Читаем файл в датафрейм


df = pd.read_excel(stg_dir + filename)


# Выбираем записи только за нужную дату


df = df[df['date'].dt.date == pd.to_datetime(curr_date).date()]


# Указываем данные, которые нужны для подключения к базе данных


db_settings = {
    'host' :'93.123.236.98',
    'database' : 'example_base',
    'user' : 'example_user',
    'password' : 'user'
}


# Делаем словарь из названий таблиц и колонок, в которые будем переливать данные


stg_tables = {
    'stg_accounts' : ['account', 'account_valid_to', 'client'],
    'stg_cards': ['card', 'account'],
    'stg_clients': ['client', 'last_name','first_name', 'patronymic', 'date_of_birth', 'passport', 'passport_valid_to', 'phone'],
    'stg_terminals': ['terminal','terminal_type','city','address'],
    'fact_transactions': ['trans_id','date','card','oper_type','amount','oper_result','terminal']
}


# Напишем функцию для формирования датафреймов с нужными колонками и затем заливаем данные в соответствующие таблицы:


def prep_load_df(table_name : str, columns_list : list, db_settings : dict):
    
    # Формируем датафрейм из списка колонок. И оставляем только уникальные строки
    stg_df = df[columns_list].drop_duplicates()
    
    # Проверяем имя таблицы. Если название начинается с 'stg_' то к датафрейму добавляем
    # колонку с датой выгрузки. Для таблицы фактов поле start_dt не добавляем
    if table_name[:4] == 'stg_':
        stg_df['start_dt'] = pd.to_datetime('now')

    # Записываем датафрейм в csv
    csv_io = io.StringIO()
    stg_df.to_csv(csv_io, sep='\t', header=False, index=False)
    csv_io.seek(0)
    
    # Соединяемся с базой. Заливаем csv в нужную таблицу
    
    conn = psycopg2.connect(**db_settings)
    gp_cursor = conn.cursor()
    gp_cursor.copy_from(csv_io, f'project.{table_name}')
    conn.commit()
    
    # Закрываем соединение
    conn.close()


# Применяем функцию ко всем таблицам из словаря


for table_name, columns in stg_tables.items():
    prep_load_df(table_name, columns, db_settings)


# Перемещаем xlsx файл в папку load_completed. Если папка не создана, то создаем ее в директории stg_dir. 

complete_dir = stg_dir + 'load_completed/'
if not os.path.exists(complete_dir):
    os.makedirs(complete_dir)

os.replace(stg_dir + filename, complete_dir + filename)


# Для записи данных в таблицы измерений составим список с названиями таблиц измерений.


tablenames = ['terminals', 'cards', 'accounts', 'clients']


# Напишем функцию для отслеживания изменений записей


def scd_update_insert(table_name : str, db_settings : dict):
    # Открываем соединение. Создаем курсор
    conn = psycopg2.connect(**db_settings)
    cursor = conn.cursor()
    conn.autocommit = True
    
    # Получим список колонок для таблицы
    cursor.execute(f"select * from project.dim_{table_name}_hist dth")
    colnames = [desc[0] for desc in cursor.description]
    
    
    # Формируем запрос на update строк в таблице измерений
    update_query = f"""update project.dim_{table_name}_hist dth 
                 set end_dt = {table_name}_update_view.start_dt 
                 from project.{table_name}_update_view 
                 where {table_name}_update_view.{colnames[0]} = dth.{colnames[0]} 
                        and end_dt = '9999-12-31';"""


    # Формируем запрос на insert строк в таблицу измерений из соответствующего view   
    insert_query = f"""insert into project.dim_{table_name}_hist 
                    ({','.join([str(elem) for elem in colnames])}) 
         select {','.join([str(elem) for elem in colnames[:-1]])}, '9999-12-31'::date 
         from project.{table_name}_update_view ;"""
    
    cursor.execute(update_query)
    cursor.execute(insert_query)
    
    conn.close()


# Выполняем update/insert в цикле для всех dim таблиц
for tablename in tablenames:
    scd_update_insert(tablename, db_settings)


# Сохраняем запросы для создания отчета в переменные


fraud1_query = f""" 
select 
       ft.trans_date fraud_dt, 
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
where date(ft.trans_date) = '{curr_date}' 
    and  date(ft.trans_date) > cl.passport_valid_to;""" 



fraud2_query = f"""
select 
    ft.trans_date fraud_dt, 
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
    on c.account_num = a.account_num \
left join 
    (select * from project.dim_clients_hist where date(end_dt) = '9999-12-31') cl 
    on cl.client_id = a.client 
where date(ft.trans_date) = '{curr_date}' 
    and  date(ft.trans_date) > a.valid_to; """



fraud3_query = f"""
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
where date(ft.trans_date) = '{curr_date}')
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
    and delta_in_hours < 1 ;"""



fraud4_query = f"""
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
;"""


# Создаем функцию для выполнения запросов и записи результата в таблицу отчета


def generate_report(report_query : str, db_settings : dict):
    # Открываем соединение. Создаем курсор
    conn = psycopg2.connect(**db_settings)
    cursor = conn.cursor()
    conn.autocommit = True
    
    # Формируем запрос на insert строк в отчет
    insert_query = f"""
                    INSERT INTO project.report(fraud_dt, passport, fio, phone, fraud_type, report_dt) 
                    {report_query}"""
    
    cursor.execute(insert_query)
    
    conn.close()


# Выполняем в цикле функцию для всех запросов


for query in [fraud1_query,fraud2_query,fraud3_query,fraud4_query]:
    generate_report(query, db_settings)

