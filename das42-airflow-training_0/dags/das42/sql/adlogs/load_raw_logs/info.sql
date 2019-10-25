create table if not exists airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.info (
  record_type string,
  date string,
  time string,
  error_severity string,
  error_number string,
  placement_id string,
  error_message string,
  ip_address string,
  referrer string,
  iab_flag string,
  user_agent string,
  file_source string,
  load_timestamp timestamp,
  run_datehour bigint
)
;

---

begin name load_rl_info_2019070415

---

delete from airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.info
where run_datehour = 2019070415
;

---

copy into airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.info from (
  select distinct
    nullif(t.$1, '-') as record_type,
    nullif(t.$2, '-') as date,
    nullif(t.$3, '-') as time,
    nullif(t.$4, '-') as error_severity,
    nullif(t.$5, '-') as error_number,
    nullif(t.$6, '-') as placement_id,
    nullif(t.$7, '-') as error_message,
    nullif(t.$8, '-') as ip_address,
    nullif(t.$9, '-') as referrer,
    nullif(t.$10, '-') as iab_flag,
    nullif(t.$11, '-') as user_agent,
    metadata$filename as file_source,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as load_timestamp,
    2019070415 as run_datehour
  from @raw_stage/stage_info_logs_{{ params.env }}/20190704/15/log/ t
)
file_format = raw_stage_{{ params.team_name }}.log_csv_nh_format
force = true
on_error = continue
;

---

delete from airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.info
where run_datehour = 2019070415
and record_type like '#Version: %'
or record_type like '#Date: %'
or record_type like '#Start-Date: %'
or record_type like '#Fields: %'
;

---

commit;
