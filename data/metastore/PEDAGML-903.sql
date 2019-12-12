merge into edag_business_date d
using (select distinct m.src_sys_cd, c.ctry_cd, m.proc_freq_cd,
to_timestamp(trunc(sysdate-1)) as prev_biz_dt,
to_timestamp(trunc(sysdate)) as curr_biz_dt,
to_timestamp(trunc(sysdate+1)) as next_biz_dt,
m.is_act_flg
from edag_process_master m
join edag_proc_ctry_dtl c
on m.proc_id = c.proc_id
) s
on (d.src_sys_cd = s.src_sys_cd and d.ctry_cd = s.ctry_cd and d.freq_cd = s.proc_freq_cd)
when not matched then insert (d.src_sys_cd, d.ctry_cd, freq_cd, d.prev_biz_dt, d.curr_biz_dt, d.next_biz_dt, d.proc_flg)
values (s.src_sys_cd, s.ctry_cd, s.proc_freq_cd, s.prev_biz_dt, s.curr_biz_dt, s.next_biz_dt, s.is_act_flg);

commit;
