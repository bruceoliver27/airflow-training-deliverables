create table if not exists airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.click (
  record_type string,
  date string,
  time string,
  idevent_type string,
  placementid string,
  ipn string,
  idcreative string,
  configuration_id string,
  GUID string,
  iab_flag string,
  ip_address string,
  rule_match string,
  custom string,
  section string,
  keyword string,
  privacy string,
  parent_time string,
  device_id string,
  imp_id string,
  agent_env string,
  user_agent string,
  impression_guid string,
  unhex_md5_smartclip string, --this will be unhex_md5_smartclip
  idcampaign string,
  c2 string,
  c3 string,
  file_source string,
  load_timestamp timestamp,
  run_datehour bigint
)
;

---

begin name load_rl_click_2019070415;

---


delete from airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.click
where run_datehour = 2019070415
;

---

copy into airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.click from (
  select distinct
    nullif(t.$1, '-') as record_type,
    nullif(t.$2, '-') as date,
    nullif(t.$3, '-') as time,
    nullif(t.$4, '-') as idevent_type,
    nullif(t.$5, '-') as placementid,
    nullif(t.$6, '-') as ipn,
    nullif(t.$7, '-') as idcreative,
    nullif(t.$8, '-') as configuration_id,
    nullif(t.$9, '-') as GUID,
    nullif(t.$10, '-') as iab_flag,
    nullif(t.$11, '-') as ip_address,
    nullif(t.$12, '-') as rule_match,
    nullif(t.$13, '-') as custom,
    nullif(t.$14, '-') as section,
    nullif(t.$15, '-') as keyword,
    nullif(t.$16, '-') as privacy,
    nullif(t.$17, '-') as parent_time,
    nullif(t.$18, '-') as device_id,
    nullif(t.$19, '-') as imp_id,
    nullif(t.$20, '-') as agent_env,
    nullif(t.$21, '-') as user_agent,
    nullif(t.$22, '-') as impression_guid,
    nullif(t.$23, '-') as tpplid,
    nullif(t.$24, '-') as idcampaign,
    nullif(t.$25, '-') as c2,
    nullif(t.$26, '-') as c3,
    metadata$filename as file_source,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as load_timestamp,
    2019070415 as run_datehour
  from @raw_stage/stage_click_logs_{{ params.env }}/20190704/15/log/ t
)
file_format = raw_stage_{{ params.team_name }}.log_csv_nh_format
on_error = continue
;

---

delete from airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.click
where run_datehour = '2019070415'
and record_type like '#Version: %'
or record_type like '#Date: %'
or record_type like '#Start-Date: %'
or record_type like '#Fields: %'
;

---

commit;
