create table if not exists airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.track (
  record_type string,
  date string,
  time string,
  event string,
  spotlight_id string,
  spotlightgroup_id string,
  track_id string,
  ip_address string,
  product string,
  data string,
  spotlight_request_guid string,
  file_source string,
  load_timestamp timestamp,
  run_datehour bigint
)
;

---

begin name load_rl_track_2019070415;

---

delete from airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.track
where run_datehour = 2019070415
;

---

copy into airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.track from (
  select distinct
    nullif(t.$1, '-') as record_type,
    nullif(t.$2, '-') as date,
    nullif(t.$3, '-') as time,
    nullif(t.$4, '-') as event,
    nullif(t.$5, '-') as spotlight_id,
    nullif(t.$6, '-') as spotlightgroup_id,
    nullif(t.$7, '-') as track_id,
    nullif(t.$8, '-') as ip_address,
    nullif(t.$9, '-') as product,
    nullif(t.$10, '-') as data,
    nullif(t.$11, '-') as spotlight_request_guid,
    metadata$filename as file_source,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as load_timestamp,
    2019070415 as run_datehour
  from @raw_stage/stage_track_logs_{{ params.env }}/20190704/15/log/ t
)
file_format = raw_stage_{{ params.team_name }}.log_csv_nh_format
on_error = continue
;

---

delete from airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.track
where run_datehour = '2019070415'
and record_type like '#Version: %'
or record_type like '#Date: %'
or record_type like '#Start-Date: %'
or record_type like '#Fields: %'
;

---

commit;
