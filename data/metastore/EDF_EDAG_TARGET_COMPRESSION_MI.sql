-- -----------------------------------------------------
-- Data for table edf.edag_target_compression_type
-- -----------------------------------------------------


INSERT INTO edag_target_compression_type (tgt_compr_type_cd, tgt_compr_type_desc, is_act_fl, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('SNP', 'Snappy', 'Y', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_target_compression_type (tgt_compr_type_cd, tgt_compr_type_desc, is_act_fl, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('ZLB', 'ZLib', 'Y', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_target_compression_type (tgt_compr_type_cd, tgt_compr_type_desc, is_act_fl, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('GZP', 'Gzip', 'Y', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_target_compression_type (tgt_compr_type_cd, tgt_compr_type_desc, is_act_fl, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('BZP', 'Bzip2', 'Y', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_target_compression_type (tgt_compr_type_cd, tgt_compr_type_desc, is_act_fl, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('LZO', 'LZO', 'N', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_target_compression_type (tgt_compr_type_cd, tgt_compr_type_desc, is_act_fl, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('LZ4', 'LZ4', 'N', DEFAULT, DEFAULT, NULL, NULL);

COMMIT;
