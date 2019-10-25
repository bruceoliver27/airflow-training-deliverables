update airflow_db_{{ params.env }}.{{ params.transform_schema }}_{{ params.name }}.{{ params.table }}
set {{ params.table }}.placementid = db.placementid
from airflow_db_{{ params.env }}.{{ params.transform_schema }}_{{ params.name }}.placement_smartclip db
where
    {{ params.table }}.unhex_md5_smartclip = db.unhex_md5_smartclip
    and {{ params.table }}.placementid < 13000001
    and {{ params.table }}.smartclip = 1;
