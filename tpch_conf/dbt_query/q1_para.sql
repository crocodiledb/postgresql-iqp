set enable_incremental to off;
set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;
set enable_dbtoaster to on;

set tpch_updates to 'lineitem';
set dbt_query to 'q1';

set tpch_delta_mode to :v_mode; 
set bd_prob to :v_prob; 

select
	q1_returnflag,
	q1_linestatus,
	sum(q1_quantity) as sum_qty,
	sum(q1_extendedprice) as sum_base_price,
	sum(q1_extendedprice*(1-q1_discount)) as sum_disc_price,
	sum(q1_extendedprice*(1-q1_discount)*(1+q1_tax)) as sum_charge,
	avg(q1_quantity) as avg_qty,
	avg(q1_extendedprice) as avg_price,
	avg(q1_discount) as avg_disc,
	count(*) as count_order
from
	q1
group by
	q1_returnflag,
	q1_linestatus
order by
	q1_returnflag,
	q1_linestatus;

