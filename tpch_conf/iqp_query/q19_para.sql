set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set decision_method to dp;

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;

set enable_incremental to on;
set tpch_updates to 'part,lineitem';
set iqp_query to 'q19';
set gen_mem_info to off; 

set memory_budget to :v_budget;
set tpch_delta_mode to :v_mode; 
set bd_prob to :v_prob; 

select
    i19_l_linenumber, sum(i19_l_extendedprice * (1 - i19_l_discount) ) as revenue
from
    i19_l,
    i19_p
where
    i19_p_partkey = i19_l_partkey
group by 
    i19_l_linenumber;

