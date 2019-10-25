--Line 344
--Business goal is to elimnate records with invalid smartclips, send those rows to an admin table for review, and
--then update placementids of the new logs (after they've been purged) by comparing the unhex codes to the larger db.

--update TYPE_CLICK to eliminate invalid smartclip rows
update airflow_db_{{ params.env }}.{{ params.transform_schema }}_{{ params.name }}.{{ params.table }}
set iab_flag = 's'
where (
    (
    placementid = 0
    or section is null )
    and iab_flag in ('w', 'x'));

---
--create admin table filled with recently purged smartclips for review
create or replace table airflow_db_{{ params.env }}.{{ params.transform_schema }}_{{ params.name }}.{{ params.table }}_purged_smartclip_rows as
(
select
    placementid,
    idcampaign,
    idevent_type,
    idcreative,
    coalesce(section,'') as section
from
    airflow_db_{{ params.env }}.{{ params.transform_schema }}_{{ params.name }}.{{ params.table }}
where
    iab_flag = 's'
);

---

--update TYPE_CLICK to set placementids for the valid smartclips. Do this by comparing the unhex codes
--between TYPE_CLICK and the larger db.
update airflow_db_{{ params.env }}.{{ params.transform_schema }}_{{ params.name }}.{{ params.table }} as c
set placementid = sc.placementid
from {{ params.transform_schema }}_{{ params.name }}.placement_smartclip sc
where
    sc.unhex_md5_smartclip = c.unhex_md5_smartclip;
