-- -----------------------------------------------------
-- Table `edf`.`edag_field_sensitive_detail`
-- -----------------------------------------------------

CREATE TABLE edag_field_sensitive_detail (
  batch_num INT NOT NULL,
  src_sys_nm VARCHAR(45) NOT NULL,
  hive_tbl_nm VARCHAR(70) NOT NULL,
  file_nm VARCHAR(45) NOT NULL,
  fld_nm VARCHAR(100) NOT NULL);

-- -----------------------------------------------------
-- Table `edf`.`edag_field_displaydeny_detail`
-- -----------------------------------------------------

CREATE TABLE edag_field_displaydeny_detail (
  batch_num INT NOT NULL,
  src_sys_nm VARCHAR(45) NOT NULL,
  hive_tbl_nm VARCHAR(70) NOT NULL,
  file_nm VARCHAR(45) NOT NULL,
  fld_nm VARCHAR(100) NOT NULL);




















