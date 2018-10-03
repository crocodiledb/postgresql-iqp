set max_parallel_workers_per_gather to 0;
set work_mem to 1000000;
set memory_budget to 1500000;
set decision_method to dp;

set enable_nestloop to off;
set enable_indexscan to off;
set enable_mergejoin to off;

set geqo to off;
set enable_sort to off;

set enable_incremental to on;
set tpch_updates to 'lineitem';
set iqp_query to 'q10';
set gen_mem_info to off; 

select
    i10_c_custname,
    sum(i10_l_extendedprice * (1 - i10_l_discount)) as revenue,
    i10_c_acctbal,
    i10_n_nationname,
    i10_c_address,
    i10_c_phone,
    i10_c_comment
from
    i10_c,
    i10_o,
    i10_l,
    i10_n
where
    i10_c_custkey = i10_o_custkey
    and i10_l_orderkey = i10_o_orderkey
    and i10_c_nationkey = i10_n_nationkey
group by
    i10_c_custname,
    i10_c_acctbal,
    i10_c_phone,
    i10_n_nationname,
    i10_c_address,
    i10_c_comment
order by
    revenue desc;

