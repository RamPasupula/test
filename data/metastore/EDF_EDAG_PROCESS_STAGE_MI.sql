-- -----------------------------------------------------
-- Data for table `edf`.`edag_process_stage`
-- -----------------------------------------------------


INSERT INTO edag_process_stage (stage_id, stage_desc, proc_type_cd, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES (211, 'Process Initialization', '21', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_process_stage (stage_id, stage_desc, proc_type_cd, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES (212, 'Data Ingestion from Edge Node to T1 HDFS', '21', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_process_stage (stage_id, stage_desc, proc_type_cd, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES (213, 'Data Ingestion from T1 HDFS to T1.1 Hive table', '21', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_process_stage (stage_id, stage_desc, proc_type_cd, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES (214, 'Process Finalization', '21', DEFAULT, DEFAULT, NULL, NULL);

-- Export Process
INSERT INTO edag_process_stage (stage_id, stage_desc, proc_type_cd, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES (711, 'Export Process Initialization', '71', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_process_stage (stage_id, stage_desc, proc_type_cd, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES (712, 'TPT Data Export from Teradata to Edge Node', '71', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_process_stage (stage_id, stage_desc, proc_type_cd, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES (713, 'Export Process Finalization', '71', DEFAULT, DEFAULT, NULL, NULL);

-- Load Process
insert into edag_process_stage (stage_id, stage_desc, proc_type_cd, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) values(331, 'Registration Process Initialization', '33', DEFAULT, DEFAULT, NULL, NULL);
insert into edag_process_stage (stage_id, stage_desc, proc_type_cd, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) values(332, 'Data Staging from T1.1 Hive Table to Hive Staging Table', '33', DEFAULT, DEFAULT, NULL, NULL);
insert into edag_process_stage (stage_id, stage_desc, proc_type_cd, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) values(333, 'TDCH Data Export from Hive Staging Table to T1.4 Teradata Table', '33', DEFAULT, DEFAULT, NULL, NULL);
insert into edag_process_stage (stage_id, stage_desc, proc_type_cd, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) values(334, 'Data Reconciliation Between T1.1 Hive Table and T1.4 Teradata Table', '33', DEFAULT, DEFAULT, NULL, NULL);

COMMIT;