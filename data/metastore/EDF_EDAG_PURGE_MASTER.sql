-------------------------------------------------------
-- Data for table edf.edag_purge_master: landing data
------------------------------------------------------

MERGE INTO edag_purge_master D
USING (select f.proc_id,m.src_sys_cd,c.ctry_cd,'L','G',7,f.file_nm,f.achv_dir_nm,'Y' from
edag_file_detail f 
inner join 
edag_process_master m on f.proc_id = m.proc_id
inner join 
edag_proc_ctry_dtl c on c.proc_id = f.proc_id) S
on (D.proc_id = S.proc_id and D.src_sys_cd = S.src_sys_cd and D.ctry_cd = S.ctry_cd and D.TIER_TYP = 'L')
WHEN MATCHED THEN UPDATE SET D.retn_typ='G',
                             D.retn_val=7,
                             D.db_nm=NULL,
                             D.tbl_nm=S.file_nm,
		             D.tbl_part_txt=NULL,
                             D.purge_dir_path=S.achv_dir_nm,
	                     D.purge_flg='Y'
	                     WHERE D.tier_typ='L'
WHEN NOT MATCHED THEN INSERT (D.proc_id,D.src_sys_cd,D.ctry_cd, D.tier_typ, D.retn_typ, D.retn_val,D.db_nm,D.tbl_nm,D.tbl_part_txt,D.purge_dir_path,D.purge_flg)
VALUES (S.proc_id,S.src_sys_cd,S.ctry_cd,'L','G',7,NULL,S.file_nm,NULL,S.achv_dir_nm,'Y');

COMMIT;
