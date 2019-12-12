-- -----------------------------------------------------
-- Data for table edf.edag_source_system
-- -----------------------------------------------------

DELETE FROM edag_source_system WHERE src_sys_cd = 'CPF';
INSERT INTO edag_source_system (src_sys_cd, src_sys_nm, is_act_flg, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('AIS', 'AIS Investment Scheme ', 'Y', DEFAULT, DEFAULT, NULL, NULL);

DELETE FROM edag_source_system WHERE src_sys_cd = 'SMF';
INSERT INTO edag_source_system (src_sys_cd, src_sys_nm, is_act_flg, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('MRG', 'Shares Margin Risk Financing System ', 'Y', DEFAULT, DEFAULT, NULL, NULL);

COMMIT;
