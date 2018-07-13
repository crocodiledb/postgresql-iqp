set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set memory_budget to 3000;
set decision_method to dp;

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;

set tpch_delta_mode to uniform; 
set bd_prob to 0.99; 

set enable_incremental to on;
set tpch_updates to 'part,lineitem';
set iqp_query to 'q14';
set gen_mem_info to off; 

select
	i14_l_linenumber, 100.00 * sum(case
		when i14_p_type like 'PROMO%'
			then i14_l_extendedprice * (1 - i14_l_discount)
		else 0
	end) / sum(i14_l_extendedprice * (1 - i14_l_discount)) as promo_revenue
from
	i14_l,
	i14_p
where
	i14_l_partkey = i14_p_partkey
group by
    i14_l_linenumber;

