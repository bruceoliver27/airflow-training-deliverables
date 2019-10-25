create table if not exists airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.spot (
  record_type string,
  date string,
  time string,
  spotlight_id string,
  advertiser_id string,
  spotlightgroup_id string,
  GUID string,
  queryArgs string,
  browser string,
  ip_address string,
  privacy string,
  track_id string,
  user_agent string,
  spotlight_request_guid string,
  file_source string,
  load_timestamp timestamp,
  run_datehour bigint
)
;

---

begin name load_rl_spot_2019070415;

---

delete from airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.spot
where run_datehour = 2019070415
;

---

copy into airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.spot from (
  select distinct
    nullif(t.$1, '-') as record_type,
    nullif(t.$2, '-') as date,
    nullif(t.$3, '-') as time,
    nullif(t.$4, '-') as spotlight_id,
    nullif(t.$5, '-') as advertiser_id,
    nullif(t.$6, '-') as spotlightgroup_id,
    nullif(t.$7, '-') as GUID,
    nullif(t.$8, '-') as queryArgs,
    nullif(t.$9, '-') as browser,
    nullif(t.$10, '-') as ip_address,
    nullif(t.$11, '-') as privacy,
    nullif(t.$12, '-') as track_id,
    nullif(t.$13, '-') as user_agent,
    nullif(t.$14, '-') as spotlight_request_guid,
    metadata$filename as file_source,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as load_timestamp,
    2019070415 as run_datehour
  from @raw_stage/stage_spot_logs_{{ params.env }}/20190704/15/log/ t
)
file_format = raw_stage_{{ params.team_name }}.log_csv_nh_format
on_error = continue
;

---

delete from airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.spot
where run_datehour = 2019070415
and record_type like '#Version: %'
or record_type like '#Date: %'
or record_type like '#Start-Date: %'
or record_type like '#Fields: %'
;

---

commit;
