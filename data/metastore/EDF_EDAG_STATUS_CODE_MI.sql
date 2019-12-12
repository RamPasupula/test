-- -----------------------------------------------------
-- Data for table edf.edag_status_code
-- -----------------------------------------------------


INSERT INTO edag_status_code (status_cd, status_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('I', 'In Progress', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_status_code (status_cd, status_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('S', 'Success', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_status_code (status_cd, status_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('F', 'Failed', DEFAULT, DEFAULT, NULL, NULL);

COMMIT;
