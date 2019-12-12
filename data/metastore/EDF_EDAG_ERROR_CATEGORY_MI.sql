-- -----------------------------------------------------
-- Data for table edf.edag_error_category
-- -----------------------------------------------------

insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB0000','General Exception');
insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB0001','Input / Output Exception');
insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB0002','Ingestion Exception');
insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB0003','Security Exception');
insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB0004','Database Access Exception');
insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB0005','Data Access Exception');
insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB0006','Remote (SSH) Exception');
insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB0007','Validation Exception');
insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB0008','XML Exception');
insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB0009','Concurrency Exception');
insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB0010','General System Exception');
insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB0011','Workflow Exception');
insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB0012','Excel Exception');
insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB0013','Processor Exception');
insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB0014','Export Exception');
insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB0015','SQL Exception');
insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB0016','Hive Exception');
insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB0017','Registration Exception');
insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB5000','Runtime Exception');
insert into edag_error_category(error_cat_cd, error_cat_desc) values('UOB5001','Class Cast Exception');

INSERT INTO edag_error_category (error_cat_cd, error_cat_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('UOB01', 'Row Count/Hashsum Validation Failure', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_error_category (error_cat_cd, error_cat_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('UOB02', 'Invalid Process Status', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_error_category (error_cat_cd, error_cat_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('UOB03', 'Invalid Process', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_error_category (error_cat_cd, error_cat_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('UOB04', 'Invalid Country', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_error_category (error_cat_cd, error_cat_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('UOB05', 'Invalid Business Date', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_error_category (error_cat_cd, error_cat_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('UOB06', 'Invalid File', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_error_category (error_cat_cd, error_cat_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('UOB07', 'BDM Error', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_error_category (error_cat_cd, error_cat_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('UOB08', 'TPT Error', DEFAULT, DEFAULT, NULL, NULL);
INSERT INTO edag_error_category (error_cat_cd, error_cat_desc, crt_dt, crt_usr_nm, upd_dt, upd_usr_nm) VALUES ('UOB99', 'System Error', DEFAULT, DEFAULT, NULL, NULL);

commit;

