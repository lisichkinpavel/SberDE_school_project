-- Запросы update/insert для реализации SCD2 на примере таблицы terminals
select * from terminals_update_view tuv ;

-- Изменяем end_dt в dim_ таблице и делаем его равным start_dt в update_view
update project.dim_terminals_hist dth  
set end_dt = terminals_update_view.start_dt    
from project.terminals_update_view        
where terminals_update_view.terminal_id = dth.terminal_id and end_dt = '9999-12-31';
      
-- Вставляем строку из update_view в dim_ таблицу и добавлем в поле end_dt значение '9999-12-31'
-- Теперь эта строка становится актуальной.

insert into project.dim_terminals_hist 
(terminal_id,terminal_type,terminal_city,terminal_adress,start_dt,end_dt)     
select terminal_id,terminal_type,terminal_city,terminal_adress,start_dt, '9999-12-31'::date   
from project.terminals_update_view ;