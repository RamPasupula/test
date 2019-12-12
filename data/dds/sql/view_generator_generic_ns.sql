select distinct ctry_cd,tbl_nm,fld_nm,fld_num from (
select  epcd.ctry_cd, elp.tgt_tbl_nm as tbl_nm,''||efid.fld_nm as fld_nm,efid.fld_num as fld_num from edag_process_master epm
inner join edag_file_detail efd
	inner join edag_field_detail efid on efid.file_id = efd.file_id
on efd.proc_id = epm.proc_id
inner join edag_load_process elp on elp.proc_id = epm.proc_id
inner join edag_proc_ctry_dtl epcd on epcd.proc_id = epm.proc_id
where not exists ( select 1 from edag_field_sensitive_detail esfd where esfd.src_sys_nm = epm.src_sys_cd and lower(elp.tgt_tbL_nm) = lower(esfd.hive_tbl_nm) and lower(efid.fld_nm) = lower(esfd.fld_nm))
and not exists (select 1 from edag_field_displaydeny_detail edfd where edfd.src_sys_nm = epm.src_sys_cd and lower(elp.tgt_tbL_nm) = lower(edfd.file_nm) and lower(efid.fld_nm) = lower(edfd.fld_nm))
and ( epm.src_sys_cd = ?)
AND (efid.RCRD_TYP_CD='FI' OR efid.RCRD_TYP_CD='PK' OR efid.RCRD_TYP_CD='FK' OR efid.RCRD_TYP_CD='BK')
UNION
select  epcd.ctry_cd, elp.tgt_tbl_nm as tbl_nm,'<NULL>'||efid.fld_nm as fld_nm,efid.fld_num as fld_num from edag_process_master epm
inner join edag_file_detail efd
	inner join edag_field_detail efid on efid.file_id = efd.file_id
on efd.proc_id = epm.proc_id
inner join edag_load_process elp on elp.proc_id = epm.proc_id
inner join edag_proc_ctry_dtl epcd on epcd.proc_id = epm.proc_id
where exists ( select 1 from edag_field_sensitive_detail esfd where esfd.src_sys_nm = epm.src_sys_cd and lower(elp.tgt_tbL_nm) = lower(esfd.hive_tbl_nm) and lower(efid.fld_nm) = lower(esfd.fld_nm))
and not exists (select 1 from edag_field_displaydeny_detail edfd where edfd.src_sys_nm = epm.src_sys_cd and lower(elp.tgt_tbL_nm) = lower(edfd.file_nm) and lower(efid.fld_nm) = lower(edfd.fld_nm))
and ( epm.src_sys_cd = ?)
AND (efid.RCRD_TYP_CD='FI' OR efid.RCRD_TYP_CD='PK' OR efid.RCRD_TYP_CD='FK' OR efid.RCRD_TYP_CD='BK')
) order by ctry_cd,tbl_nm,fld_nm,fld_num asc