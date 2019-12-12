-----------------------------------------------------------------
                 ----T1.0 data loading---
-------------------------------------------------------------------


MERGE INTO edag_purge_master D
USING (select f.proc_id,m.src_sys_cd,c.ctry_cd,'T1.0','G',7,f.stg_db_nm,f.stg_tbl_nm,f.stg_tbl_part_txt,f.stg_dir_nm,'Y' from
edag_load_process f 
inner join 
edag_process_master m on f.proc_id = m.proc_id
inner join 
edag_proc_ctry_dtl c on c.proc_id = f.proc_id) S
ON (D.proc_id = S.proc_id and D.src_sys_cd = S.src_sys_cd and D.ctry_cd = S.ctry_cd and D.TIER_TYP = 'T1.0')
WHEN MATCHED THEN UPDATE SET D.db_nm = S.stg_db_nm,
                             D.retn_typ = 'G',
                             D.retn_val = 7,
                             D.tbl_nm = S.stg_tbl_nm,
                             D.tbl_part_txt = S.stg_tbl_part_txt,
                             D.purge_dir_path = S.stg_dir_nm,
                             D.PURGE_FLG = 'Y'
                             WHERE D.TIER_TYP   = 'T1.0'
WHEN NOT MATCHED THEN INSERT (D.proc_id,D.src_sys_cd,D.ctry_cd, D.tier_typ, D.retn_typ, D.retn_val,D.db_nm,D.tbl_nm,D.tbl_part_txt,D.purge_dir_path,D.purge_flg)
VALUES (S.proc_id,S.src_sys_cd,S.ctry_cd,'T1.0','G',7,S.stg_db_nm,S.stg_tbl_nm,S.stg_tbl_part_txt,S.stg_dir_nm,'Y');

COMMIT;
