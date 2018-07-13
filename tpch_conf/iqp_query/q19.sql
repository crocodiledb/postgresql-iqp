set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set memory_budget to 10000;
set decision_method to dp;

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;

set tpch_delta_mode to uniform; 
set bd_prob to 0.99; 

set enable_incremental to on;
set tpch_updates to 'part,lineitem';
set iqp_query to 'q19';
set gen_mem_info to off; 

select
    i19_l_linenumber, sum(i19_l_extendedprice * (1 - i19_l_discount) ) as revenue
from
    i19_l,
    i19_p
where
    i19_p_partkey = i19_l_partkey
group by 
    i19_l_linenumber;

