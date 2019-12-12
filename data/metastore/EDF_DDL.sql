-- -----------------------------------------------------
-- Create Schema edf
-- -----------------------------------------------------

--CREATE USER edf IDENTIFIED BY hadoop;

--GRANT CONNECT, RESOURCE, DBA TO edf;

-- -----------------------------------------------------
-- Table `edf`.`edag_source_system`
-- -----------------------------------------------------

CREATE TABLE edag_source_system (
  src_sys_cd VARCHAR(10) NOT NULL,
  src_sys_nm VARCHAR(45) NOT NULL,
  is_act_flg CHAR(1) NOT NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (src_sys_cd));
  
-- -----------------------------------------------------
-- Table edag_process_type
-- -----------------------------------------------------
CREATE TABLE edag_process_type (
  proc_type_cd VARCHAR(10) NOT NULL,
  proc_type_nm VARCHAR(45) NOT NULL,
  proc_type_desc VARCHAR(100) NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (proc_type_cd));

-- -----------------------------------------------------
-- Table edag_target_format_type
-- -----------------------------------------------------
CREATE TABLE edag_target_format_type (
  tgt_format_cd VARCHAR(10) NOT NULL,
  tgt_format_desc VARCHAR(100) NOT NULL,
  is_act_flg CHAR(1) NOT NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (tgt_format_cd));


-- -----------------------------------------------------
-- Table edag_target_compression_type
-- -----------------------------------------------------
CREATE TABLE edag_target_compression_type (
  tgt_compr_type_cd VARCHAR(10) NOT NULL,
  tgt_compr_type_desc VARCHAR(100) NOT NULL,
  is_act_fl CHAR(1) NOT NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (tgt_compr_type_cd));

-- -----------------------------------------------------
-- Table edag_target_apply_type
-- -----------------------------------------------------
CREATE TABLE edag_target_apply_type (
  tgt_apply_type_cd VARCHAR(10) NOT NULL,
  tgt_apply_type_desc VARCHAR(100) NOT NULL,
  is_act_flg CHAR(1) NOT NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (tgt_apply_type_cd));

-- -----------------------------------------------------
-- Table edag_process_group
-- -----------------------------------------------------
CREATE TABLE edag_process_group (
  proc_grp_id INT NOT NULL,
  proc_grp_nm VARCHAR(45) NOT NULL,
  proc_grp_desc VARCHAR(100) NOT NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (proc_grp_id));

-- -----------------------------------------------------
-- Table edag_process_master
-- -----------------------------------------------------
CREATE TABLE edag_process_master (
  proc_id VARCHAR(50) NOT NULL,
  proc_nm VARCHAR(50) NOT NULL,
  proc_type_cd VARCHAR(10) NOT NULL,
  proc_grp_id INT NULL,
  proc_desc VARCHAR(2000) NULL,
  proc_freq_cd VARCHAR(10) NOT NULL,
  src_sys_cd VARCHAR(10) NULL,
  deploy_node_nm VARCHAR(45) NOT NULL,
  proc_criticality_cd VARCHAR(10) NULL,
  is_act_flg CHAR(1) NOT NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) NOT NULL,
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (proc_id),
  CONSTRAINT fk_proc_mstr_src_sys_cd
    FOREIGN KEY (src_sys_cd)
    REFERENCES edag_source_system (src_sys_cd),
  CONSTRAINT fk_proc_mstr_proc_type_cd
    FOREIGN KEY (proc_type_cd)
    REFERENCES edag_process_type (proc_type_cd),
  CONSTRAINT fk_proc_mstr_proc_grp_id
    FOREIGN KEY (proc_grp_id)
    REFERENCES edag_process_group (proc_grp_id));
    
CREATE SEQUENCE "SEQ_PROC_ID" MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;
CREATE INDEX src_sys_cd_idx ON edag_process_master (src_sys_cd ASC);
CREATE INDEX proc_type_cd_idx ON edag_process_master (proc_type_cd ASC);
CREATE INDEX proc_grp_id_idx ON edag_process_master (proc_grp_id ASC);



-- -----------------------------------------------------
-- Table edag_load_process
-- -----------------------------------------------------
CREATE TABLE edag_load_process (
  proc_id VARCHAR(50) NOT NULL,
  tgt_dir_nm VARCHAR(75) NULL,
  tgt_format_cd VARCHAR(10) NULL,
  tgt_compr_type_cd VARCHAR(10) NULL,
  tgt_aply_type_cd VARCHAR(10) NOT NULL,
  tgt_db_nm VARCHAR(70) NULL,
  tgt_tbl_nm VARCHAR(70) NULL,
  tgt_tbl_part_txt VARCHAR(70) NULL,
  stg_dir_nm VARCHAR(90) NULL,
  stg_db_nm VARCHAR(70) NULL,
  stg_tbl_nm VARCHAR(70) NULL,
  stg_tbl_part_txt VARCHAR(45) NULL,
  err_threshold NUMBER NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) NOT NULL,
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (proc_id),
  CONSTRAINT fk_ld_proc_tgt_format_cd
    FOREIGN KEY (tgt_format_cd)
    REFERENCES edag_target_format_type (tgt_format_cd),
  CONSTRAINT fk_ld_proc_tgt_compr_type_cd
    FOREIGN KEY (tgt_compr_type_cd)
    REFERENCES edag_target_compression_type (tgt_compr_type_cd),
  CONSTRAINT fk_ld_proc_id
    FOREIGN KEY (proc_id)
    REFERENCES edag_process_master (proc_id),
  CONSTRAINT fk_ld_proc_tgt_apply_type_cd
    FOREIGN KEY (tgt_aply_type_cd)
    REFERENCES edag_target_apply_type (tgt_apply_type_cd));

CREATE INDEX tgt_format_cd_idx ON edag_load_process (tgt_format_cd ASC);
CREATE INDEX tgt_compr_type_cd_idx ON edag_load_process (tgt_compr_type_cd ASC);
CREATE INDEX tgt_apply_type_cd_idx ON edag_load_process (tgt_aply_type_cd ASC);

-- -----------------------------------------------------
-- Table edag_file_type
-- -----------------------------------------------------
CREATE TABLE edag_file_type (
  file_type_cd VARCHAR(10) NOT NULL,
  file_type_desc VARCHAR(100) NOT NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (file_type_cd));


-- -----------------------------------------------------
-- Table edag_file_layout
-- -----------------------------------------------------
CREATE TABLE edag_file_layout (
  file_layout_cd VARCHAR(10) NOT NULL,
  file_layout_desc VARCHAR(100) NOT NULL,
  is_act_fl CHAR(1) NOT NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (file_layout_cd));

ALTER TABLE edag_file_layout MODIFY file_layout_cd VARCHAR2(30);

-- -----------------------------------------------------
-- Table edag_control_file_detail
-- -----------------------------------------------------
CREATE TABLE edag_control_file_detail (
  ctrl_file_id INT NOT NULL,
  ctrl_file_dir_txt VARCHAR(150) NOT NULL,
  ctrl_file_nm VARCHAR(45) NOT NULL,
  ctrl_file_extn_nm VARCHAR(10) NULL,
  PRIMARY KEY (ctrl_file_id));
  
CREATE SEQUENCE "SEQ_CTRL_FILE_ID" MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;

alter table edag_control_file_detail add ctrl_file_layout_cd VARCHAR(10) NULL;
alter table edag_control_file_detail add ctrl_file_col_delim_txt VARCHAR(10) NULL;
alter table edag_control_file_detail add ctrl_file_txt_delim_txt CHAR(1) NULL;

ALTER TABLE edag_control_file_detail ADD ctrl_file_explicit_dec_point char(1) null;


-- -----------------------------------------------------
-- Table edag_file_detail
-- -----------------------------------------------------
CREATE TABLE edag_file_detail (
  file_id INT NOT NULL,
  proc_id VARCHAR(50) NOT NULL,
  dir_nm VARCHAR(100) NOT NULL,
  file_nm VARCHAR(45) NOT NULL,
  file_extn_nm VARCHAR(10) NULL,
  ctrl_file_id INT NULL,
  ctrl_info_cd VARCHAR(10) NULL,
  file_type_cd VARCHAR(10) NOT NULL,
  file_layout_cd VARCHAR(10) NOT NULL,
  file_col_delim_txt VARCHAR(10) NULL,
  file_txt_delim_txt CHAR(1) NULL,
  hdr_line_num INT NOT NULL,
  ftr_line_num INT NOT NULL,
  achv_dir_nm VARCHAR(150) NOT NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) NULL,
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (file_id),
  CONSTRAINT fk_file_dtl_file_type_cd
    FOREIGN KEY (file_type_cd)
    REFERENCES edag_file_type (file_type_cd),
  CONSTRAINT fk_file_dtl_file_layout_cd
    FOREIGN KEY (file_layout_cd)
    REFERENCES edag_file_layout (file_layout_cd),
  CONSTRAINT fk_file_dtl_proc_id
    FOREIGN KEY (proc_id)
    REFERENCES edag_load_process (proc_id));

ALTER TABLE edag_file_detail ADD file_explicit_dec_point char(1) null;
ALTER TABLE edag_file_detail MODIFY file_layout_cd VARCHAR2(30);
ALTER TABLE edag_file_detail ADD use_spark_based_ingestion char(1) DEFAULT 'N';

CREATE SEQUENCE "SEQ_FILE_ID" MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;
CREATE INDEX proc_id_idx ON edag_file_detail (proc_id ASC);
CREATE INDEX ctrl_file_id_idx ON edag_file_detail (ctrl_file_id ASC);
CREATE INDEX fk_file_type_cd_idx ON edag_file_detail (file_type_cd ASC);
CREATE INDEX fk_file_layout_cd_idx ON edag_file_detail (file_layout_cd ASC);


-- -----------------------------------------------------
-- Table edag_field_detail
-- -----------------------------------------------------
CREATE TABLE edag_field_detail (
  file_id INT NOT NULL,
  rcrd_typ_cd VARCHAR(10) NOT NULL,
  fld_nm VARCHAR(100) NOT NULL,
  fld_desc VARCHAR(4000) NULL,
  fld_num INT NOT NULL,
  fld_len_num VARCHAR(10) NOT NULL,
  fld_dec_prec INT NULL,
  fld_data_type_txt VARCHAR(20) NOT NULL,
  fld_format_txt VARCHAR(30) NULL,
  fld_def_val VARCHAR(100) NULL,
  fld_start_pos_num INT NULL,
  fld_end_pos_num INT NULL,
  is_fld_hashsum_flg CHAR(1) NULL,
  is_fld_index_flg CHAR(1) NULL,
  is_fld_profile_flg CHAR(1) NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) NOT NULL,
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (file_id, rcrd_typ_cd, fld_nm),
  CONSTRAINT fk_fld_dtl_file_id
    FOREIGN KEY (file_id)
    REFERENCES edag_file_detail (file_id));

ALTER TABLE edag_field_detail
ADD
  fld_optionality CHAR(1);

alter table edag_field_detail add fld_biz_term varchar2(4000);
alter table edag_field_detail add fld_biz_definition varchar2(4000);
alter table edag_field_detail add fld_synonyms varchar2(4000);
alter table edag_field_detail add fld_usage_context varchar2(4000);
alter table edag_field_detail add fld_system_steward varchar2(4000);
alter table edag_field_detail add fld_source_system varchar2(4000); 
alter table edag_field_detail add fld_source_table varchar2(4000);
alter table edag_field_detail add fld_source_field_name varchar2(4000);
alter table edag_field_detail add fld_source_field_desc varchar2(4000);
alter table edag_field_detail add fld_source_field_type varchar2(4000);
alter table edag_field_detail add fld_source_field_length int;
alter table edag_field_detail add fld_source_field_format varchar2(4000);
alter table edag_field_detail add fld_source_data_category varchar2(4000);
alter table edag_field_detail add fld_lov_code_and_desc varchar2(4000);
alter table edag_field_detail add fld_optionality_2 varchar2(4000);
alter table edag_field_detail add fld_sysdata_validation_logic varchar2(4000);
alter table edag_field_detail add fld_data_availability varchar2(4000);
ALTER TABLE edag_field_detail ADD REGULAR_EXPRESSION VARCHAR2(255);

-- -----------------------------------------------------
-- Table edag_status_code
-- -----------------------------------------------------
CREATE TABLE edag_status_code (
  status_cd VARCHAR(10) NOT NULL,
  status_desc VARCHAR(100) NOT NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (status_cd));


-- -----------------------------------------------------
-- Table edag_uob_country
-- -----------------------------------------------------
CREATE TABLE edag_uob_country (
  ctry_cd VARCHAR(10) NOT NULL,
  ctry_nm VARCHAR(20) NOT NULL,
  is_act_fl CHAR(1) NOT NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (ctry_cd));

-- -----------------------------------------------------
-- Table edag_error_category
-- -----------------------------------------------------
CREATE TABLE edag_error_category (
  error_cat_cd VARCHAR(10) NOT NULL,
  error_cat_desc VARCHAR(100) NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (error_cat_cd));

-- -----------------------------------------------------
-- Table edag_process_log
-- -----------------------------------------------------
CREATE TABLE edag_process_log (
  proc_instance_id VARCHAR(45) NOT NULL,
  biz_dt DATE NOT NULL,
  proc_id VARCHAR(50) NOT NULL,
  src_sys_cd VARCHAR(10) NULL,
  ctry_cd VARCHAR(10) NULL,
  file_nm VARCHAR(255) NULL,
  proc_start_dt DATE NULL,
  proc_end_dt DATE NULL,
  proc_status_cd VARCHAR(10) NOT NULL,
  proc_error_cat_cd VARCHAR(10) NULL,
  proc_error_txt VARCHAR(2000) NULL,
  PRIMARY KEY (proc_instance_id),
  CONSTRAINT fk_proc_log_status_cd
    FOREIGN KEY (proc_status_cd)
    REFERENCES edag_status_code (status_cd),
  CONSTRAINT fk_proc_log_src_sys_cd
    FOREIGN KEY (src_sys_cd)
    REFERENCES edag_source_system (src_sys_cd),
  CONSTRAINT fk_proc_log_ctry_cd
    FOREIGN KEY (ctry_cd)
    REFERENCES edag_uob_country (ctry_cd),
  CONSTRAINT fk_proc_log_error_cat_cd
    FOREIGN KEY (proc_error_cat_cd)
    REFERENCES edag_error_category (error_cat_cd));

CREATE INDEX fk_status_cd_idx ON edag_process_log (proc_status_cd ASC);
CREATE INDEX fk_src_sys_cd_idx ON edag_process_log (src_sys_cd ASC);
CREATE INDEX fk_ctry_cd_idx ON edag_process_log (ctry_cd ASC);
CREATE INDEX fk_error_cat_cd_idx ON edag_process_log (proc_error_cat_cd ASC);

ALTER TABLE edag_process_log ADD HOUR_RUN VARCHAR2(10);

-- -----------------------------------------------------
-- Table edag_process_stage
-- -----------------------------------------------------
CREATE TABLE edag_process_stage (
  stage_id INT NOT NULL,
  stage_desc VARCHAR(100) NOT NULL,
  proc_type_cd VARCHAR(10) NOT NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (stage_id),
  CONSTRAINT fk_proc_type_cd
    FOREIGN KEY (proc_type_cd)
    REFERENCES edag_process_type (proc_type_cd));

CREATE INDEX proc_stg_prc_type_cd_idx ON edag_process_stage (proc_type_cd ASC);

-- -----------------------------------------------------
-- Table edag_process_stage_log
-- -----------------------------------------------------
CREATE TABLE edag_process_stage_log (
  stage_id INT NOT NULL,
  proc_instance_id VARCHAR(45) NOT NULL,
  stage_start_dt DATE NULL,
  stage_end_dt DATE NULL,
  stage_status_cd VARCHAR(10) NOT NULL,
  stage_error_cat_cd VARCHAR(10) NULL,
  stage_error_txt VARCHAR(2000) NULL,
  hashsum_fld VARCHAR(100),
  src_hashsum_amt NUMBER NULL,
  tgt_hashsum_amt NUMBER NULL,
  rows_input INT NULL,
  rows_considered INT NULL,
  rows_not_considered INT NULL,
  rows_inserted INT NULL,
  rows_deleted INT NULL,
  rows_updated INT NULL,
  rows_rejected INT NULL,
  rows_et INT NULL,
  rows_uv INT NULL,
  db_session_id INT NULL,
  tool_session_id INT NULL,
  tool_proc_id VARCHAR(45) NULL,
  upd_dt DATE DEFAULT (sysdate),
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (stage_id, proc_instance_id),
  CONSTRAINT fk_stg_log_proc_instance_id
    FOREIGN KEY (proc_instance_id)
    REFERENCES edag_process_log (proc_instance_id),
  CONSTRAINT fk_stg_log_stage_id
    FOREIGN KEY (stage_id)
    REFERENCES edag_process_stage (stage_id),
  CONSTRAINT fk_stg_log_status_cd
    FOREIGN KEY (stage_status_cd)
    REFERENCES edag_status_code (status_cd),
  CONSTRAINT fk_stg_log_error_cat_cd
    FOREIGN KEY (stage_error_cat_cd)
    REFERENCES edag_error_category (error_cat_cd));

CREATE INDEX proc_instance_id_idx ON edag_process_stage_log (proc_instance_id ASC);
CREATE INDEX fk_stage_id_idx ON edag_process_stage_log (stage_id ASC);
CREATE INDEX fk_stage_status_cd_idx ON edag_process_stage_log (stage_status_cd ASC);
CREATE INDEX fk_prc_stg_error_cat_cd_idx ON edag_process_stage_log (stage_error_cat_cd ASC);


-- -----------------------------------------------------
-- Table edag_unit_test_details
-- -----------------------------------------------------
--CREATE TABLE edag_unit_test_details (
--  tst_id VARCHAR(26) NOT NULL,
--  proc_id VARCHAR(50) NOT NULL,
--  tst_start_dt DATE NULL,
--  tst_end_dt DATE NULL,
--  tst_status CHAR(1) NOT NULL,
--  tst_error_msg VARCHAR(255) NULL,
--  tst_init_usr VARCHAR(10) NOT NULL,
--  PRIMARY KEY (tst_id),
--  CONSTRAINT fk_tst_dtl_proc_id
--    FOREIGN KEY (proc_id)
--    REFERENCES edag_process_master (proc_id),
--  CONSTRAINT fk_tst_dtl_status_cd
--    FOREIGN KEY (tst_status)
--    REFERENCES edag_status_code (status_cd));

--CREATE INDEX fk_ut_proc_id_idx ON edag_unit_test_details (proc_id ASC);

--CREATE INDEX fk_ut_status_cd_idx ON edag_unit_test_details (tst_status ASC);

-- -----------------------------------------------------
-- Table edag_audit_detail
-- -----------------------------------------------------
CREATE TABLE edag_audit_detail (
  audit_id INT NOT NULL,
  audit_dt DATE NOT NULL,
  usr_nm VARCHAR(10) NOT NULL,
  usr_action_txt VARCHAR(255) NULL,
  status_cd VARCHAR(10) NULL,
  PRIMARY KEY (audit_id),
  CONSTRAINT fk_audit_status_cd
    FOREIGN KEY (status_cd)
    REFERENCES edag_status_code (status_cd));

CREATE SEQUENCE "SEQ_AUDIT_ID" MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;

CREATE INDEX fk_audit_status_cd_idx ON edag_audit_detail (status_cd ASC);

-- -----------------------------------------------------
-- Table edag_alerts
-- -----------------------------------------------------
CREATE TABLE edag_alerts (
  alert_id INT NOT NULL,
  proc_id VARCHAR(50) NOT NULL,
  alert_email VARCHAR(100) NOT NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) NOT NULL,
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (alert_id),
  CONSTRAINT fk_alert_proc_id
    FOREIGN KEY (proc_id)
    REFERENCES edag_process_master (proc_id));

CREATE INDEX fk_alrt_proc_id_idx ON edag_alerts (proc_id ASC);

CREATE SEQUENCE "SEQ_ALERT_ID" MINVALUE 1 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 1;

-- -----------------------------------------------------
-- Table edag_field_standardization_rule
-- -----------------------------------------------------
CREATE TABLE edag_std_rule (
  rule_id INT NOT NULL,
  rule_desc VARCHAR(100) NOT NULL,
  is_act_fl CHAR(1) NOT NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (rule_id));

-- -----------------------------------------------------
-- Table edag_field_std_rules
-- -----------------------------------------------------
CREATE TABLE edag_field_std_rules (
  file_id INT NOT NULL,
  fld_nm VARCHAR(100) NOT NULL,
  rcrd_typ_cd VARCHAR(10) NOT NULL,
  rule_id INT NOT NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) NOT NULL,
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (file_id, fld_nm, rcrd_typ_cd, rule_id),
  CONSTRAINT fk_fld_std_rule_id
    FOREIGN KEY (rule_id)
    REFERENCES edag_std_rule (rule_id),
  CONSTRAINT fk_fld_std_file_id
    FOREIGN KEY (file_id, fld_nm, rcrd_typ_cd)
    REFERENCES edag_field_detail (file_id, fld_nm, rcrd_typ_cd));

CREATE INDEX fk_rule_id_idx ON edag_field_std_rules (rule_id ASC);

CREATE INDEX fk_fld_nm_idx ON edag_field_std_rules (fld_nm ASC);

CREATE INDEX fk_rcrd_typ_cd_idx ON edag_field_std_rules (rcrd_typ_cd ASC);

-- -----------------------------------------------------
-- Table edag_downstream_appl
-- -----------------------------------------------------
CREATE TABLE edag_downstream_appl (
  appl_cd VARCHAR(10) NOT NULL,
  appl_nm VARCHAR(45) NOT NULL,
  appl_desc VARCHAR(255) NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (appl_cd));


-- -----------------------------------------------------
-- Table edag_proc_downstream_appl
-- -----------------------------------------------------
CREATE TABLE edag_proc_downstream_appl (
  proc_id VARCHAR(50) NOT NULL,
  appl_cd VARCHAR(10) NOT NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (proc_id),
  CONSTRAINT fk_dwnstrm_appl_proc_id
    FOREIGN KEY (proc_id)
    REFERENCES edag_process_master (proc_id),
  CONSTRAINT fk_dwnstrm_appl_appl_cd
    FOREIGN KEY (appl_cd)
    REFERENCES edag_downstream_appl (appl_cd));

CREATE INDEX fk_appl_cd_idx ON edag_proc_downstream_appl (appl_cd ASC);


-- -----------------------------------------------------
-- Table edag_schedules
-- -----------------------------------------------------
CREATE TABLE edag_schedules (
  proc_id VARCHAR(50) NOT NULL,
  sch_nm VARCHAR(45) NOT NULL,
  sch_freq VARCHAR(50) NULL,
  sch_time DATE NULL,
  sch_dependency VARCHAR(45) NULL,
  sch_predecessor VARCHAR(45) NULL,
  sch_successor VARCHAR(45) NULL,
  sch_script VARCHAR(255) NOT NULL,
  PRIMARY KEY (proc_id),
  CONSTRAINT fk_sch_proc_id
    FOREIGN KEY (proc_id)
    REFERENCES edag_process_master (proc_id));

-- -----------------------------------------------------
-- Table edag_proc_param
-- -----------------------------------------------------
CREATE TABLE edag_proc_param (
  proc_id VARCHAR(50) NOT NULL,
  param_nm VARCHAR(45) NOT NULL,
  param_val VARCHAR(500) NOT NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) DEFAULT 'SYSTEM',
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (proc_id,param_nm),
  CONSTRAINT fk_proc_id
    FOREIGN KEY (proc_id)
    REFERENCES edag_process_master (proc_id));


-- -----------------------------------------------------
-- Table edag_proc_ctry_dtl
-- -----------------------------------------------------
CREATE TABLE edag_proc_ctry_dtl (
  proc_id VARCHAR(50) NOT NULL,
  ctry_cd VARCHAR(10) NOT NULL,
  is_act_flg CHAR(1) NULL,
  upd_dt DATE DEFAULT (sysdate),
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (proc_id, ctry_cd),
  CONSTRAINT fk_proc_ctry_proc_id
    FOREIGN KEY (proc_id)
    REFERENCES edag_process_master (proc_id),
  CONSTRAINT fk_proc_ctry_ctry_cd
    FOREIGN KEY (ctry_cd)
    REFERENCES edag_uob_country (ctry_cd));

CREATE INDEX fk_proc_ctry_cd_idx ON edag_proc_ctry_dtl (ctry_cd ASC);

-- -----------------------------------------------------
-- Table edag_export_process
-- -----------------------------------------------------
CREATE TABLE edag_export_process (
  proc_id VARCHAR(50) NOT NULL,
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
  crt_usr_nm VARCHAR(10) NOT NULL,
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (proc_id),
  CONSTRAINT fk_exp_proc_id
    FOREIGN KEY (proc_id)
    REFERENCES edag_process_master (proc_id));


-- -----------------------------------------------------
-- Table edag_business_date
-- -----------------------------------------------------
CREATE TABLE edag_business_date (
  src_sys_cd VARCHAR(10) NOT NULL,
  ctry_cd VARCHAR(10) NOT NULL,
  freq_cd VARCHAR(10) NOT NULL,
  prev_biz_dt DATE NULL,
  curr_biz_dt DATE NULL,
  next_biz_dt DATE NULL,
  proc_flg CHAR(1) NOT NULL,
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (src_sys_cd, ctry_cd, freq_cd),
  CONSTRAINT fk_biz_dt_src_sys_idx
    FOREIGN KEY (src_sys_cd)
    REFERENCES edag_source_system (src_sys_cd),
  CONSTRAINT fk_biz_dt_uob_ctry_idx
    FOREIGN KEY (ctry_cd)
    REFERENCES edag_uob_country (ctry_cd));

CREATE INDEX fk_biz_dt_src_sys_idx ON edag_business_date (src_sys_cd ASC);
CREATE INDEX fk_biz_dt_uob_ctry_idx ON edag_business_date (ctry_cd ASC);

ALTER TABLE edag_business_date ADD HOUR_TO_RUN VARCHAR2(10);


-- -----------------------------------------------------
-- Table edag_proc_holiday
-- -----------------------------------------------------
CREATE TABLE edag_proc_holiday (
  src_sys_cd VARCHAR(10) NOT NULL,
  ctry_cd VARCHAR(10) NOT NULL,
  holiday_dt DATE NULL,
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (src_sys_cd, ctry_cd),
  CONSTRAINT fk_proc_holiday_src_sys_idx
    FOREIGN KEY (src_sys_cd)
    REFERENCES edag_source_system (src_sys_cd),
  CONSTRAINT fk_proc_holiday_uob_ctry_idx
    FOREIGN KEY (ctry_cd)
    REFERENCES edag_uob_country (ctry_cd));

CREATE INDEX fk_proc_holiday_src_sys_idx ON edag_proc_holiday (src_sys_cd ASC);
CREATE INDEX fk_proc_holiday_uob_ctry_idx ON edag_proc_holiday (ctry_cd ASC);


-- -----------------------------------------------------
-- Table edag_source_table_detail
-- -----------------------------------------------------
CREATE TABLE EDAG_SOURCE_TABLE_DETAIL
(
    SRC_DB_CONNECTION_NM VARCHAR2(128),
    SRC_SCHEMA_NM VARCHAR2(128),
    SRC_TBL_NM VARCHAR2(128),
    PROC_ID VARCHAR2(50),
    CRT_DT TIMESTAMP (6),
    CRT_USR_NM VARCHAR2(10),
    UPD_DT TIMESTAMP (6),
    UPD_USR_NM VARCHAR2(10),
    PRIMARY KEY (SRC_DB_CONNECTION_NM, SRC_SCHEMA_NM, SRC_TBL_NM, PROC_ID)
);


-- -----------------------------------------------------
-- Table edag_purge_master
-- -----------------------------------------------------

CREATE TABLE edag_purge_master (
purge_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) NOT NULL,
proc_id VARCHAR(50) NOT NULL,
src_sys_cd VARCHAR(10) NULL,
ctry_cd VARCHAR(10) NULL,
tier_typ varchar(20) NOT NULL,
retn_typ varchar(20) NOT NULL,
retn_val integer,
db_nm VARCHAR(70) NULL,
tbl_nm VARCHAR(70)  NULL,
tbl_part_txt VARCHAR(70) NULL,
purge_dir_path VARCHAR(150)  NULL,
purge_flg VARCHAR(2) NOT NULL,
PRIMARY KEY (purge_id)
);

-- -----------------------------------------------------
-- Table edag_field_sensitive_detail
-- -----------------------------------------------------

CREATE TABLE edag_field_sensitive_detail (
  batch_num INT NOT NULL,
  src_sys_nm VARCHAR(45) NOT NULL,
  hive_tbl_nm VARCHAR(70) NOT NULL,
  file_nm VARCHAR(45) NOT NULL,
  fld_nm VARCHAR(100) NOT NULL);

-- -----------------------------------------------------
-- Table edag_field_displaydeny_detail
-- -----------------------------------------------------

CREATE TABLE edag_field_displaydeny_detail (
  batch_num INT NOT NULL,
  src_sys_nm VARCHAR(45) NOT NULL,
  hive_tbl_nm VARCHAR(70) NOT NULL,
  file_nm VARCHAR(45) NOT NULL,
  fld_nm VARCHAR(100) NOT NULL);

-- -----------------------------------------------------
-- Table edag_field_name_patterns
-- ----------------------------------------------------- 
CREATE TABLE edag_field_name_patterns (
  file_id INT NOT NULL,
  fld_nm VARCHAR(100) NOT NULL,
  rcrd_typ_cd VARCHAR(10) NOT NULL,
  proc_id VARCHAR(50) NOT NULL,
  field_name_pattern VARCHAR(100) NOT NULL,
  crt_dt DATE DEFAULT (sysdate),
  crt_usr_nm VARCHAR(10) NOT NULL,
  upd_dt DATE NULL,
  upd_usr_nm VARCHAR(10) NULL,
  PRIMARY KEY (file_id, fld_nm, rcrd_typ_cd, field_name_pattern),
  CONSTRAINT fk_proc_id
    FOREIGN KEY (proc_id)
    REFERENCES edag_process_master (proc_id),
  CONSTRAINT fk_fld_nm_patterns_file_id
    FOREIGN KEY (file_id, fld_nm, rcrd_typ_cd)
    REFERENCES edag_field_detail (file_id, fld_nm, rcrd_typ_cd));


-- -----------------------------------------------------
-- Table edag_lob_stamping_sql_info
-- ----------------------------------------------------- 
CREATE TABLE edag_lob_stamping_sql_info (
  proc_id VARCHAR(50) NOT NULL,
  lob_stamping_sql CLOB,
  site_id_compute_sql CLOB,
  dependant_tables VARCHAR(1000) NOT NULL,
  hive_table_name VARCHAR(150) NOT NULL,
  PRIMARY KEY (proc_id),
  CONSTRAINT fk_lob_proc_id
    FOREIGN KEY (proc_id)
    REFERENCES edag_process_master (proc_id)
);
    

-- -----------------------------------------------------
-- Table edag_lob_lookup_update
-- ----------------------------------------------------- 
CREATE TABLE edag_lob_lookup_update (
    lookup_table_name VARCHAR2(60) NOT NULL,
    lob_update_sql_id VARCHAR2(20) NOT NULL,
    lob_update_sql LONG NOT NULL,
    column_names VARCHAR2(200) NOT NULL,
    dependent_tables VARCHAR2(300) NOT NULL,
    hbase_table_name VARCHAR2(50) NOT NULL,
    status VARCHAR
    CONSTRAINT lookup_update_pk PRIMARY KEY
    (lookup_table_name, lob_update_sql_id)
);