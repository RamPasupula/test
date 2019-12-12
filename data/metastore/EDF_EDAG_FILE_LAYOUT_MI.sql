-- -----------------------------------------------------
-- Data for table edf.edag_file_layouts
-- -----------------------------------------------------


INSERT INTO edag_file_layout (file_layout_cd, file_layout_desc, is_act_fl, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('FXD', 'Fixed', 'Y', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_file_layout (file_layout_cd, file_layout_desc, is_act_fl, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('DEL', 'Delimited', 'Y', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_file_layout (file_layout_cd, file_layout_desc, is_act_fl, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) values ('XlsxWHeader', 'Xlsx Without Header', 'Y', DEFAULT, DEFAULT,null,null);
INSERT INTO edag_file_layout (file_layout_cd, file_layout_desc, is_act_fl, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) values ('XlsxWOHeader', 'Xlsx With Header', 'Y', DEFAULT, DEFAULT, null, null);
INSERT INTO edag_file_layout (file_layout_cd, file_layout_desc, is_act_fl, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) values ('XlsWHeader', 'Xls With Header', 'Y', DEFAULT, DEFAULT, null, null);
INSERT INTO edag_file_layout (file_layout_cd, file_layout_desc, is_act_fl, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) values ('XlsWOHeader', 'Xls Without Header', 'Y', DEFAULT, DEFAULT, null, null);

COMMIT;
