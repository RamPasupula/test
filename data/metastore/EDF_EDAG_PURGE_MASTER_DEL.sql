
----------------------------------------------------------------
            ---Delete proc ids that are not in process master
------------------------------------------------------------------

DELETE
FROM edag_purge_master
WHERE proc_id NOT IN
  ( SELECT proc_id FROM edag_process_master
  );

COMMIT;
