set enable_incremental to off;
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;
set enable_dbtoaster to on;

set tpch_updates to 'part,lineitem';
set dbt_query to 'q14';

set tpch_delta_mode to :v_mode; 
set bd_prob to :v_prob; 

select
	q14_linenumber, 100.00 * sum(case
		when q14_type like 'PROMO%'
			then q14_extendedprice * (1 - q14_discount)
		else 0
	end) / sum(q14_extendedprice * (1 - q14_discount)) as promo_revenue
from
	q14
group by
    q14_linenumber;


