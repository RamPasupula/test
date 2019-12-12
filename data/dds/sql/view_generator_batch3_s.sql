select ctry_cd,tbl_nm,fld_nm,fld_num from (
select  epcd.ctry_cd, elp.tgt_tbl_nm as tbl_nm,''||efid.fld_nm as fld_nm,efid.fld_num as fld_num from edag_process_master epm
inner join edag_file_detail efd
	inner join edag_field_detail efid on efid.file_id = efd.file_id
on efd.proc_id = epm.proc_id
inner join edag_load_process elp on elp.proc_id = epm.proc_id
inner join edag_proc_ctry_dtl epcd on epcd.proc_id = epm.proc_id
where not exists (select 1 from edag_field_displaydeny_detail edfd where edfd.src_sys_nm = epm.src_sys_cd and lower(elp.tgt_tbL_nm) = lower(edfd.hive_tbl_nm) and lower(efid.fld_nm) = lower(edfd.fld_nm))
and ( epm.src_sys_cd in ('BLS', 'CCR', 'COD', 'GTD', 'IOS', 'NPL', 'REM','RBK','WSS') /* batch 3 */
    or epm.src_sys_cd in ('GLN') and elp.tgt_tbl_nm not like 'EDA_GLN_%_ID' escape '\' )
AND (efid.RCRD_TYP_CD='FI' OR efid.RCRD_TYP_CD='PK' OR efid.RCRD_TYP_CD='FK' OR efid.RCRD_TYP_CD='BK')
) order by tbl_nm,fld_nm,fld_num asc