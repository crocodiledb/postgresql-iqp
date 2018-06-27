set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set memory_budget to 1500000;
set decision_method to dp;

set enable_incremental to off;
set tpch_updates to 'lineitem';
set iqp_query to 'q3';
set gen_mem_info to off; 

refresh q3_refresh with no data; 
