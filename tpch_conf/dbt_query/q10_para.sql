set enable_incremental to off;
set tpch_updates to 'customer,orders,lineitem';
set max_parallel_workers_per_gather to 0;
set work_mem to 2000000;

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;
set dbt_query to 'q10';
set enable_dbtoaster to on;

set tpch_delta_mode to :v_mode; 
set bd_prob to :v_prob; 

select
    q10_custname,
    sum(q10_extendedprice * (1 - q10_discount)) as revenue,
    q10_acctbal,
    q10_nationname,
    q10_address,
    q10_phone,
    q10_comment
from
    q10
group by
    q10_custname,
    q10_acctbal,
    q10_phone,
    q10_nationname,
    q10_address,
    q10_comment
order by
    revenue desc;

