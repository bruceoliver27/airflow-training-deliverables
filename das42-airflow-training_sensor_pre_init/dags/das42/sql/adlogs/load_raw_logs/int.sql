drop table if exists airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.int;

---

create table if not exists airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.int (
  ipa string,
  date_field string,
  time_field string,
  agent_string string,
  idad_index string,
  idcreative_config string,
  placementid string,
  smartclip string,
  idevent_type string,
  sid string,
  guid string,
  unhex_md5_smartclip string,
  options string,
  file_source string,
  load_timestamp string,
  run_datehour bigint
)
;

---

begin name load_rl_int_2019070415;

---

delete from airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.int
where run_datehour = 2019070415
;

---

copy into airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.int from (
  select distinct
    t.$1 as ipa,
    t.$2 as date_field,
    t.$3 as time_field,
    t.$4 as agent_string,
    t.$5 as idad_index,
    t.$6 as idcreative_config,
    t.$7 as placementid,
    t.$8 as smartclip,
    t.$9 as idevent_type,
    t.$10 as sid,
    t.$11 as guid,
    t.$12 as unhex_md5_smartclip,
    t.$13 as options,
    t.$14 as file_source,
    t.$15 as load_timestamp,
    t.$16 as run_datehour
  from @raw_stage/stage_int_logs_{{ params.env }}/20190704/15/log/ t
)
file_format = raw_stage_{{ params.team_name }}.log_csv_nh_format
on_error = continue
;

---

commit;
