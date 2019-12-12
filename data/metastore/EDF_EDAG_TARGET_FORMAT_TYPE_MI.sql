-- -----------------------------------------------------
-- Data for table edf.edag_target_format_types
-- -----------------------------------------------------


INSERT INTO edag_target_format_type (tgt_format_cd, tgt_format_desc, is_act_flg, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('PRQ', 'Parquet', 'Y', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_target_format_type (tgt_format_cd, tgt_format_desc, is_act_flg, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('ORC', 'ORC', 'Y', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_target_format_type (tgt_format_cd, tgt_format_desc, is_act_flg, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('SEQ', 'Sequence', 'Y', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_target_format_type (tgt_format_cd, tgt_format_desc, is_act_flg, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('TXT', 'Text', 'Y', DEFAULT, DEFAULT, NULL, NULL);

COMMIT;