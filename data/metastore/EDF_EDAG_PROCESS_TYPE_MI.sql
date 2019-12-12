-- -----------------------------------------------------
-- Data for table edf.edag_process_type
-- -----------------------------------------------------


INSERT INTO edag_process_type (proc_type_cd, proc_type_nm, proc_type_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('11', 'PP_XX', 'Preprocessing before file ingestion or loading', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_process_type (proc_type_cd, proc_type_nm, proc_type_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('21', 'FI-STANDARD', 'File Ingestion processing to Data Lake', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_process_type (proc_type_cd, proc_type_nm, proc_type_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('22', 'FI-PARAMETER', 'Parameter File Ingestion Process', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_process_type (proc_type_cd, proc_type_nm, proc_type_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('23', 'FI-XML', 'XML File Ingestion Process', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_process_type (proc_type_cd, proc_type_nm, proc_type_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('31', 'LD-TPT_FASTLOAD', 'Loading data to EDW via TPT FastLoad', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_process_type (proc_type_cd, proc_type_nm, proc_type_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('32', 'LD-TPT_MULTILOAD', 'Loading data to EDW via TPT Multiload', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_process_type (proc_type_cd, proc_type_nm, proc_type_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('33', 'LD-TDCH', 'TDCH Loading from Data Lake to EDW using TDCH', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_process_type (proc_type_cd, proc_type_nm, proc_type_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('51', 'TX-EDW_TRANSACTIONAL', 'Transactional Transformation Process in Teradata database', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_process_type (proc_type_cd, proc_type_nm, proc_type_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('52', 'TX-EDW_DELTA', 'Delta Transformation process in Teradata database', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_process_type (proc_type_cd, proc_type_nm, proc_type_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('53', 'TX-EDW_FULL', 'Full Transformation process in Teradata database', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_process_type (proc_type_cd, proc_type_nm, proc_type_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('71', 'EX-TPT_FASTEXPORT', 'TPT Fast Export process', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_process_type (proc_type_cd, proc_type_nm, proc_type_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('72', 'EX-TDCH', 'TDCH Export from EDW to Data Lake', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_process_type (proc_type_cd, proc_type_nm, proc_type_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('91', 'OP-DL_PURGE', 'Data Lake data purge process', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_process_type (proc_type_cd, proc_type_nm, proc_type_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('92', 'OP-DL_PROCESS_REPORT', 'Data Lake Process Status Report', DEFAULT, DEFAULT, NULL, NULL);

COMMIT;
