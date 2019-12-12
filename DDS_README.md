# EDF

## Teradata's Enterprise Data Framework for UOB

### Data Discovery Service PII Registration 

  #### dds_pii_register is used for registering the Sensitive Fields.
  
  The script accepts three variables as input SensitiveFields Excel File, TruncateInsert , Excel file Column Indices.

    ./dds_pii_register.sh /prodlib_dev/specs/DDS/EDAG_SensitiveData_Batch1_2_20170731.xlsx true 0,1,2,4,7,14

### Data Discovery Service View Generation

   #### dds_generate_views is the script that is used to generate the views.

    ./dds_generate_views.sh COMMAND SQL_PATH SRC_SYSTEM 
 
  1. The command for the generate views are ns_views/s_views .
  
        ns_views -- Stands for generating the Non-Sensitive views.
  
        s_views -- Stands for generating the Sensitive Views.
        
  2. The view generation uses the EDAG_FIELD_SENSITIVE_DETAIL and EDAG_FIELD_DISPLAY_DENY_DETAIL tables data
     for loading the sensitive fields and non-displayable fields.
 
  3. There are few templates for the view generation already in the project directory Structure. 
  
      view_generator_batch3_ns.sql
      view_generator_batch3_s.sql
      view_generator_batch12_ns.sql
      view_generator_batch12_s.sql
      view_generator_generic_ns.sql
      view_generator_generic_s.sql
 
      The view generator SQL file will be having the information based on the batches.
      
  4. TO generate the non-sensitive views for batch 1 and 2 please provide as below:
       
      <code>./dds_generate_views.sh ns_views ../sql/view_generator_batch12_ns.sql all </code>
      
      To generate the sensitive views for batch 1 and 2 please provide as below:
      
      <code>./dds_generate_views.sh s_views ../sql/view_generator_batch12_s.sql all </code>
      
  5. However, if we want to generate views by source system, we need to use view_generator_generic_ns and view_generator_generic_s 
  for non-sensitive and sensitive-views respectively.
  
      <code>./dds_generate_views.sh ns_views ../sql/view_generator_generic_ns.sql BWC </code>
      
      <code>./dds_generate_views.sh s_views ../sql/view_generator_generic_s.sql BWC </code>





