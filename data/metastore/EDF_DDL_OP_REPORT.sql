
  CREATE OR REPLACE  VIEW V_DAILY_REPORT_STATUS_ALL_JOBS (BIZ_DT, TOTAL_NO_DAILY_JOBS, TOTAL_NO_SUCCESSFUL_JOBS, TOTAL_NO_INPROGRESS_JOBS, TOTAL_NO_FAILED_JOBS) AS 
  select biz_dt,(TOTAL_NO_SUCCESSFUL_JOBS + TOTAL_NO_INPROGRESS_JOBS + TOTAL_NO_FAILED_JOBS) as
TOTAL_NO_DAILY_JOBS,TOTAL_NO_SUCCESSFUL_JOBS,TOTAL_NO_INPROGRESS_JOBS,TOTAL_NO_FAILED_JOBS
from (select biz_dt,proc_status_cd from (select ll.biz_dt,ll.proc_status_cd,ll.proc_id,ll.src_sys_cd,ll.ctry_cd,ll.proc_instance_id from edag_process_log ll inner join EDAG_FILE_DETAIL FF ON FF.PROC_ID = ll.PROC_ID group by ll.biz_dt,ll.proc_status_cd,ll.proc_id,ll.src_sys_cd,ll.ctry_cd,ll.proc_instance_id ))
pivot(count(*) for proc_status_cd in ('S'  TOTAL_NO_SUCCESSFUL_JOBS,'I'
TOTAL_NO_INPROGRESS_JOBS ,'F' TOTAL_NO_FAILED_JOBS )) order by biz_dt;

  CREATE OR REPLACE  VIEW V_FAILURE_REPORT (BIZ_DT, COUNTRY_CODE, JOB_NAME, PROCESS_ID, FILE_NAME, START_TIME, END_TIME, STATUS, FAILURE_REASON) AS 
  SELECT
BIZ_DT, COUNTRY_CODE, JOB_NAME, PROCESS_ID, FILE_NAME, START_TIME
, END_TIME, STATUS, FAILURE_REASON
FROM
(
SELECT
AA.BIZ_DT AS BIZ_DT
,AA.CTRY_CD AS COUNTRY_CODE
,AA.PROC_ID AS JOB_NAME
,AA.PROC_INSTANCE_ID AS PROCESS_ID
,FF.FILE_NM AS FILE_NAME
,AA.proc_start_dt
,to_char(aa.proc_start_dt, 'dd-mon-yyyy hh24:mi:ss')  AS START_TIME
,to_char(PROC_END_DT, 'dd-mon-yyyy hh24:mi:ss') AS END_TIME
,AA.PROC_STATUS_CD AS STATUS
,AA.PROC_ERROR_TXT AS FAILURE_REASON
FROM EDAG_PROCESS_LOG AA
 INNER JOIN EDAG_FILE_DETAIL FF ON FF.PROC_ID = AA.PROC_ID)
 where status = 'F' order by biz_dt,file_name;
 
 CREATE OR REPLACE  VIEW V_FILE_RECEPTION (COUNTRY_CODE, BIZ_DT, SOURCE_SYSTEM, FILE_FREQUENCY, FILE_TYPE, FILE_NAME, DATE_TIME_FILE_ARRIVAL, ROWS_INPUT, ROWS_INSERTED) AS 
  select
country_code, biz_dt, source_system, file_frequency, file_type, file_name
, DATE_TIME_FILE_ARRIVAL, rows_input, rows_inserted
FROM
(
SELECT
aa.ctry_cd as country_code
,aa.biz_dt as biz_dt
,aa.src_sys_cd as source_system
,bb.proc_freq_cd as file_frequency
,dd.file_type_desc as file_type
,aa.file_nm as file_name
,AA.proc_start_dt
,MAX(to_char(aa.proc_start_dt, 'dd-mon-yyyy hh24:mi:ss')) over (partition BY aa.proc_id) AS DATE_TIME_FILE_ARRIVAL
,ee.stage_id, MAX(ee.stage_id) over (partition by AA.proc_instance_id) as STAGE_IDTM
,ee.rows_input
,ee.rows_inserted
from edag_process_log aa
inner join edag_process_master bb on aa.proc_id = bb.proc_id
inner join edag_file_detail cc on aa.proc_id = cc.proc_id
inner join edag_file_type dd on cc.file_type_cd = dd.file_type_cd
inner join edag_process_stage_log ee on aa.proc_instance_id = ee.proc_instance_id
)
WHERE (DATE_TIME_FILE_ARRIVAL >= to_char(proc_start_dt, 'dd-mon-yyyy hh24:mi:ss') OR (DATE_TIME_FILE_ARRIVAL IS NULL AND to_char(proc_start_dt, 'dd-mon-yyyy hh24:mi:ss') IS NULL)) and (STAGE_IDTM = stage_id or (STAGE_IDTM is null and stage_id is null));


CREATE OR REPLACE  VIEW V_LOADING_COMP_BIZ (COUNTRY_CODE, SOURCE_SYSTEM, FILE_NAME, PROC_START_DT, DATE_TIME_FILE_ARRIVAL, HIVE_TABLE_NAME, DATE_TIME_HIVE_TABLE_LOAD, LOADING_STATUS, BIZ_DT, ROWS_INPUT, ROWS_INSERTED) AS 
  SELECT
  country_code, source_system, file_name,proc_start_dt,DATE_TIME_FILE_ARRIVAL,
  HIVE_TABLE_NAME, DATE_TIME_HIVE_TABLE_LOAD, LOADING_STATUS,
  BIZ_DT, ROWS_INPUT, ROWS_INSERTED
FROM(
SELECT
AA.CTRY_CD AS COUNTRY_CODE
,AA.SRC_SYS_CD AS SOURCE_SYSTEM
,FF.FILE_NM AS FILE_NAME
,AA.proc_start_dt
,(to_char(aa.proc_start_dt, 'dd-mon-yyyy hh24:mi:ss'))  AS DATE_TIME_FILE_ARRIVAL
,BB.TGT_TBL_NM AS HIVE_TABLE_NAME
,AA.PROC_INSTANCE_ID
,AA.PROC_ID
,AA.BIZ_DT
,AA.PROC_STATUS_CD AS LOADING_STATUS
,(CASE WHEN AA.PROC_STATUS_CD = 'F' THEN '-' ELSE TMP.DATE_TIME_HIVE_TABLE_LOAD END) DATE_TIME_HIVE_TABLE_LOAD
,(CASE WHEN AA.PROC_STATUS_CD = 'F' THEN 0 ELSE TMP.rows_input  END) rows_input
,(CASE WHEN AA.PROC_STATUS_CD = 'F' THEN 0 ELSE TMP.rows_inserted  END) rows_inserted
from edag_process_log  AA
INNER JOIN EDAG_FILE_DETAIL FF ON FF.PROC_ID = AA.PROC_ID
INNER JOIN edag_load_process BB ON AA.PROC_ID = BB.PROC_ID
LEFT OUTER JOIN (select max(stage_id),proc_instance_id,max(to_char(stage_end_dt, 'dd-mon-yyyy hh24:mi:ss')) DATE_TIME_HIVE_TABLE_LOAD,
rows_input,rows_inserted from edag_process_stage_log where stage_id IN (select stg_id  from (select max(stage_id) as stg_id,proc_type_cd from edag_process_stage group by proc_type_cd))
group by proc_instance_id,rows_input,rows_inserted) TMP ON TMP.proc_instance_id = AA.PROC_INSTANCE_ID
order by file_name,proc_start_dt,biz_dt );

 CREATE OR REPLACE  VIEW V_LOADING_COMP_PS (COUNTRY_CODE, SOURCE_SYSTEM, FILE_NAME, PROC_START_DT, DATE_TIME_FILE_ARRIVAL, HIVE_TABLE_NAME, DATE_TIME_HIVE_TABLE_LOAD, CURRENT_LOADING_STAGE, LOADING_STATUS, BIZ_DT, ROWS_INPUT, ROWS_INSERTED) AS 
  SELECT
  country_code, source_system, file_name,proc_start_dt,DATE_TIME_FILE_ARRIVAL,
  HIVE_TABLE_NAME, DATE_TIME_HIVE_TABLE_LOAD, CURRENT_LOADING_STAGE,
  LOADING_STATUS,  BIZ_DT, ROWS_INPUT, ROWS_INSERTED
FROM
  (SELECT AA.CTRY_CD AS COUNTRY_CODE
,AA.SRC_SYS_CD AS SOURCE_SYSTEM
,AA.FILE_NM AS FILE_NAME
,AA.proc_start_dt
,MAX(to_char(aa.proc_start_dt, 'dd-mon-yyyy hh24:mi:ss')) over (partition BY aa.proc_id) AS DATE_TIME_FILE_ARRIVAL
,CC.stage_id, MAX(CC.stage_id) over (partition by AA.proc_instance_id) as STAGE_IDTM
,BB.TGT_TBL_NM AS HIVE_TABLE_NAME
,to_char(bb.crt_dt, 'dd-mon-yyyy hh24:mi:ss') AS DATE_TIME_HIVE_TABLE_LOAD
,DD.STAGE_DESC AS CURRENT_LOADING_STAGE
,AA.PROC_STATUS_CD AS LOADING_STATUS
,AA.BIZ_DT AS BIZ_DT
--,MAX(aa.biz_dt) over (partition BY aa.proc_start_dt) AS BIZ_DT
,CC.ROWS_INPUT
,CC.ROWS_INSERTED
FROM edag_process_log AA
  INNER JOIN edag_load_process BB  ON AA.PROC_ID = BB.PROC_ID
  inner JOIN edag_process_stage_log CC  ON AA.proc_instance_id = CC.proc_instance_id
  inner JOIN edag_process_stage DD ON CC.stage_id = DD.stage_id
  )
WHERE (DATE_TIME_FILE_ARRIVAL >= to_char(proc_start_dt, 'dd-mon-yyyy hh24:mi:ss') OR (DATE_TIME_FILE_ARRIVAL IS NULL AND to_char(proc_start_dt, 'dd-mon-yyyy hh24:mi:ss') IS NULL)) and (STAGE_IDTM = stage_id or (STAGE_IDTM is null and stage_id is null));

