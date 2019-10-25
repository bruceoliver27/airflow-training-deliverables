update airflow_db_{{ params.env }}.{{ params.transform_schema }}_{{ params.name }}.{{ params.table }}
set iab_flag = 'e'
from
    airflow_db_{{ params.env }}.{{ params.transform_schema }}_{{ params.name }}.ipn_exclude
where
    ipn_exclude.ipn_range_end = 0
    and {{ params.table }}.ipn is not null
    and {{ params.table }}.ipn = ipn_exclude.ipn;

---

update airflow_db_{{ params.env }}.{{ params.transform_schema }}_{{ params.name }}.{{ params.table }}
set iab_flag = 'e'
from (
        select
            ipn
          , ipn_range_end
        from 
        	airflow_db_{{ params.env }}.{{ params.transform_schema }}_{{ params.name }}.ipn_exclude
        where 
        	ipn_range_end <> 0
        ) as ipn_range
where 
	{{ params.table }}.ipn between ipn_range.ipn and ipn_range.ipn_range_end
;