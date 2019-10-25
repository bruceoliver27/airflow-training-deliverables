update airflow_db_{{ params.env }}.{{ params.transform_schema }}_{{ params.name }}.{{ params.table }}
set iab_flag = 'e'
from
    airflow_db_{{ params.env }}.{{ params.transform_schema }}_{{ params.name }}.ipn_exclude
where
    ipn_exclude.ipn_range_end = 0
    and {{ params.table }}.ipn is not null
    and {{ params.table }}.ipn = ipn_exclude.ipn;
    