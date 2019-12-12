REM INSERTING into EDAG_FIELD_DISPLAYDENY_DETAIL
SET DEFINE OFF;
delete from EDAG_FIELD_DISPLAYDENY_DETAIL where BATCH_NUM = 4;
INSERT
INTO EDAG_FIELD_DISPLAYDENY_DETAIL
  (
    BATCH_NUM,
    SRC_SYS_NM,
    HIVE_TBL_NM,
    FILE_NM,
    FLD_NM
  )
select distinct 3 as batch_num,epm.src_sys_cd as SRC_SYS_NM , elp.tgt_tbl_nm as hive_tbl_nm, efd.file_nm as file_nm, efid.fld_nm as fld_nm from edag_process_master epm
inner join edag_file_detail efd
        inner join edag_field_detail efid on efid.file_id = efd.file_id
on efd.proc_id = epm.proc_id
inner join edag_load_process elp on elp.proc_id = epm.proc_id
inner join edag_proc_ctry_dtl epcd on epcd.proc_id = epm.proc_id
where lower(efid.fld_nm) in ('record type','record_type','record_type_ind','Record ID')
    and efid.fld_num = 1
    and
    (
    epm.src_sys_cd in ('AIS', 'ERM', 'MRG', 'RMS', 'SBS', 'SRS', 'VSA') /* batch 4 */
    or
    epm.src_sys_cd in ('CCS', 'CMC', 'FCG', 'LSG', 'VAT') /* batch 4 to be confirmed */
    or
    (
    epm.src_sys_cd in ('BWC', 'COD', 'FIS', 'FIT', 'GLN', 'GTD', 'LES', 'LNS', 'NPL', 'RBK', 'RLP', 'WSS') /* Non batch 4 Source systems for ID, RMS and VSA are already in batch 4 list*/
    and elp.tgt_tbl_nm not like 'EDA_'||epm.src_sys_cd||'_%_ID' escape '\'
    )
    )