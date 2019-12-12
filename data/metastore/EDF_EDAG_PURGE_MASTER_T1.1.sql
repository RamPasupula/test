--------------------------------------------------------------
--------- To load Staging T1.1 data
-------------------------------------------------------------- 

MERGE INTO edag_purge_master D
USING (select f.proc_id,m.src_sys_cd,c.ctry_cd,'T1.1',p.proc_freq_cd,f.tgt_db_nm,f.tgt_tbl_nm,f.tgt_tbl_part_txt,NULL,'Y' from
edag_load_process f 
inner join 
edag_process_master m on f.proc_id = m.proc_id
inner join 
edag_proc_ctry_dtl c on c.proc_id = f.proc_id
inner join
edag_process_master p on p.proc_id = f.proc_id) S
on (D.proc_id = S.proc_id and D.src_sys_cd = S.src_sys_cd and D.ctry_cd = S.ctry_cd and D.TIER_TYP = 'T1.1')
WHEN MATCHED THEN UPDATE SET D.retn_typ='G',
                             D.retn_val=1830,
                             D.tbl_nm=S.tgt_tbl_nm,
			     D.db_nm=S.tgt_db_nm,
                             D.tbl_part_txt=S.tgt_tbl_part_txt,
			     D.purge_dir_path=NULL,
			     D.purge_flg='Y'
			     WHERE D.tier_typ='T1.1'
WHEN NOT MATCHED THEN INSERT (D.proc_id,D.src_sys_cd,D.ctry_cd, D.tier_typ, D.retn_typ, D.retn_val,D.db_nm,D.tbl_nm,D.tbl_part_txt,D.purge_dir_path,D.purge_flg)
VALUES (S.proc_id,S.src_sys_cd,S.ctry_cd,'T1.1','G',1830,S.tgt_db_nm,S.tgt_tbl_nm,S.tgt_tbl_part_txt,NULL,'Y');

COMMIT;
