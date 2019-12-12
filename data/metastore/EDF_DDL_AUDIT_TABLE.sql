CREATE TABLE edag_process_group_audit (
  proc_grp_id INT  NULL,
  proc_grp_nm VARCHAR(45)  NULL,
  proc_grp_desc VARCHAR(100)  NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  audt_dt DATE NULL );
  
CREATE TABLE edag_process_master_audit (
 proc_id VARCHAR(50) NULL,
 proc_nm VARCHAR(50) NULL,
 proc_type_cd VARCHAR(10) NULL,
 proc_grp_id INT NULL,
 proc_desc VARCHAR(2000) NULL,
 proc_freq_cd VARCHAR(10) NULL,
 src_sys_cd VARCHAR(10) NULL,
 deploy_node_nm VARCHAR(45) NULL,
 proc_criticality_cd VARCHAR(10) NULL,
 is_act_flg CHAR(1)  NULL,
 crt_dt DATE DEFAULT (sysdate),
 crt_usr_nm VARCHAR(10)  NULL,
 upd_dt DATE NULL,
 upd_usr_nm VARCHAR(10) NULL,
 audt_dt DATE NULL )
  ;

CREATE INDEX proc_id_audit_idx ON edag_process_master_audit (proc_id ASC);   
CREATE INDEX src_sys_cd_audit_idx ON edag_process_master_audit (src_sys_cd ASC);
CREATE INDEX proc_type_cd_audit_idx ON edag_process_master_audit (proc_type_cd ASC);
CREATE INDEX proc_grp_id_audit_idx ON edag_process_master_audit (proc_grp_id ASC);

CREATE TABLE edag_load_process_audit (
  proc_id VARCHAR(50)  NULL,
  tgt_dir_nm VARCHAR(75) NULL,
  tgt_format_cd VARCHAR(10) NULL,
  tgt_compr_type_cd VARCHAR(10) NULL,
  tgt_aply_type_cd VARCHAR(10)  NULL,
  tgt_db_nm VARCHAR(70) NULL,
  tgt_tbl_nm VARCHAR(70) NULL,
  tgt_tbl_part_txt VARCHAR(70) NULL,
  stg_dir_nm VARCHAR(90) NULL,
  stg_db_nm VARCHAR(70) NULL,
  stg_tbl_nm VARCHAR(70) NULL,
  stg_tbl_part_txt VARCHAR(45) NULL,
  err_threshold NUMBER NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10)  NULL,
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  audt_dt DATE NULL
 );

CREATE INDEX proc_id_load_audit_idx ON edag_load_process_audit (proc_id ASC); 
CREATE INDEX tgt_format_cd_audit_idx ON edag_load_process_audit (tgt_format_cd ASC);
CREATE INDEX tgt_compr_type_cd_audit_idx ON edag_load_process_audit (tgt_compr_type_cd ASC);
CREATE INDEX tgt_apply_type_cd_audit_idx ON edag_load_process_audit (tgt_aply_type_cd ASC);
  

CREATE TABLE edag_export_process_audit (
  proc_id VARCHAR(50)  NULL,
  src_db_nm VARCHAR(70) NULL,
  src_tbl_nm VARCHAR(70) NULL,
  tgt_dir_nm VARCHAR(100) NULL,
  tgt_file_nm VARCHAR(70) NULL,
  tgt_file_extn_nm VARCHAR(10) NULL,
  tgt_file_col_delim_txt CHAR(1) NULL,
  tgt_file_txt_delim_txt CHAR(1) NULL,
  ctl_file_nm VARCHAR(70) NULL,
  ctl_file_extn_nm VARCHAR(10) NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10)  NULL,
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  audt_dt DATE NULL
 );
 CREATE INDEX proc_id_export_audit_idx ON edag_export_process_audit (proc_id ASC); 
 
 CREATE TABLE edag_proc_dwnstrm_appl_audit (
  proc_id VARCHAR(50) NULL,
  appl_cd VARCHAR(10)  NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  audt_dt DATE NULL
 );

CREATE INDEX proc_id_downstream_audit_idx ON edag_proc_dwnstrm_appl_audit (proc_id ASC);
CREATE INDEX fk_appl_cd_audit_idx ON edag_proc_dwnstrm_appl_audit (appl_cd ASC);

CREATE TABLE edag_proc_param_audit (
  proc_id VARCHAR(50)  NULL,
  param_nm VARCHAR(45)  NULL,
  param_val VARCHAR(500)  NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
   audt_dt DATE NULL
 );
CREATE INDEX proc_id_proc_audit_idx ON edag_proc_param_audit (proc_id ASC,param_nm ASC); 


CREATE TABLE edag_uob_country_audit (
  ctry_cd VARCHAR(10)  NULL,
  ctry_nm VARCHAR(20)  NULL,
  is_act_fl CHAR(1)  NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
   audt_dt DATE NULL
 );
  
 CREATE INDEX uob_country_audit_idx ON edag_uob_country_audit (ctry_cd ASC); 
 
 
 
 CREATE TABLE edag_file_detail_audit (
  file_id INT  NULL,
  proc_id VARCHAR(50)  NULL,
  dir_nm VARCHAR(100)  NULL,
  file_nm VARCHAR(45)  NULL,
  file_extn_nm VARCHAR(10) NULL,
  ctrl_file_id INT NULL,
  ctrl_info_cd VARCHAR(10) NULL,
  file_type_cd VARCHAR(10)  NULL,
  file_layout_cd VARCHAR(10)  NULL,
  file_col_delim_txt VARCHAR(10) NULL,
  file_txt_delim_txt CHAR(1) NULL,
  hdr_line_num INT  NULL,
  ftr_line_num INT  NULL,
  achv_dir_nm VARCHAR(150)  NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) NULL,
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  audt_dt DATE NULL
 );

ALTER TABLE edag_file_detail_audit ADD file_explicit_dec_point char(1) null;
ALTER TABLE edag_file_detail_audit MODIFY file_layout_cd VARCHAR2(30);

CREATE INDEX file_detail_audit_idx ON edag_file_detail_audit (file_id ASC);
CREATE INDEX file_detail_proc_idx ON edag_file_detail_audit (proc_id ASC);
CREATE INDEX ctrl_file_id_audit_idx ON edag_file_detail_audit (ctrl_file_id ASC);
CREATE INDEX fk_file_type_cd_audit_idx ON edag_file_detail_audit (file_type_cd ASC);
CREATE INDEX fk_file_layout_cd_audit_idx ON edag_file_detail_audit (file_layout_cd ASC);



CREATE TABLE edag_field_detail_audit (
  file_id INT  NULL,
  rcrd_typ_cd VARCHAR(10)  NULL,
  fld_nm VARCHAR(100)  NULL,
  fld_desc VARCHAR(4000) NULL,
  fld_num INT  NULL,
  fld_len_num VARCHAR(10)  NULL,
  fld_dec_prec INT NULL,
  fld_data_type_txt VARCHAR(20)  NULL,
  fld_format_txt VARCHAR(30) NULL,
  fld_def_val VARCHAR(100) NULL,
  fld_start_pos_num INT NULL,
  fld_end_pos_num INT NULL,
  is_fld_hashsum_flg CHAR(1) NULL,
  is_fld_index_flg CHAR(1) NULL,
  is_fld_profile_flg CHAR(1) NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10)  NULL,
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  audt_dt DATE NULL
 );
	
CREATE INDEX field_detail_audit_idx ON edag_field_detail_audit (file_id ASC,rcrd_typ_cd ASC,fld_nm ASC);	

alter table edag_field_detail_audit add fld_optionality char(1);
alter table edag_field_detail_audit add fld_biz_term varchar2(4000);
alter table edag_field_detail_audit add fld_biz_definition varchar2(4000);
alter table edag_field_detail_audit add fld_synonyms varchar2(4000);
alter table edag_field_detail_audit add fld_usage_context varchar2(4000);
alter table edag_field_detail_audit add fld_system_steward varchar2(4000);
alter table edag_field_detail_audit add fld_source_system varchar2(4000); 
alter table edag_field_detail_audit add fld_source_table varchar2(4000);
alter table edag_field_detail_audit add fld_source_field_name varchar2(4000);
alter table edag_field_detail_audit add fld_source_field_desc varchar2(4000);
alter table edag_field_detail_audit add fld_source_field_type varchar2(4000);
alter table edag_field_detail_audit add fld_source_field_length int;
alter table edag_field_detail_audit add fld_source_field_format varchar2(4000);
alter table edag_field_detail_audit add fld_source_data_category varchar2(4000);
alter table edag_field_detail_audit add fld_lov_code_and_desc varchar2(4000);
alter table edag_field_detail_audit add fld_optionality_2 varchar2(4000);
alter table edag_field_detail_audit add fld_sysdata_validation_logic varchar2(4000);
alter table edag_field_detail_audit add fld_data_availability varchar2(4000);


CREATE TABLE edag_control_file_detail_audit (
  ctrl_file_id INT  NULL,
  ctrl_file_dir_txt VARCHAR(150)  NULL,
  ctrl_file_nm VARCHAR(45)  NULL,
  ctrl_file_extn_nm VARCHAR(10) NULL,
  audt_dt DATE NULL
 );
 
 CREATE INDEX control_file_detail_audit_idx ON edag_control_file_detail_audit (ctrl_file_id ASC);

alter table edag_control_file_detail_audit add ctrl_file_layout_cd VARCHAR(10) NULL;
alter table edag_control_file_detail_audit add ctrl_file_col_delim_txt VARCHAR(10) NULL;
alter table edag_control_file_detail_audit add ctrl_file_txt_delim_txt CHAR(1) NULL;

ALTER TABLE edag_control_file_detail_audit ADD ctrl_file_explicit_dec_point char(1) null;


 CREATE TABLE edag_std_rule_audit (
  rule_id INT  NULL,
  rule_desc VARCHAR(100)  NULL,
  is_act_fl CHAR(1)  NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  audt_dt DATE NULL
 );
  
  CREATE INDEX std_rule_audit_idx ON edag_std_rule_audit (rule_id ASC);
  
  
  CREATE TABLE edag_field_std_rules_audit (
  file_id INT  NULL,
  fld_nm VARCHAR(100)  NULL,
  rcrd_typ_cd VARCHAR(10)  NULL,
  rule_id INT  NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10)  NULL,
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  audt_dt DATE NULL
 );

CREATE INDEX field_std_rules_audit_idx ON edag_field_std_rules_audit (file_id ASC,fld_nm ASC,rcrd_typ_cd ASC,rule_id ASC);	
CREATE INDEX fk_rule_id_audit_idx ON edag_field_std_rules_audit (rule_id ASC);
CREATE INDEX fk_fld_nm_audit_idx ON edag_field_std_rules_audit (fld_nm ASC);
CREATE INDEX fk_rcrd_typ_cd_audit_idx ON edag_field_std_rules_audit (rcrd_typ_cd ASC);

