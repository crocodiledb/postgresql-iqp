#!/bin/bash

BENCH_HOME=/home/totemtang/IQP/postgresql/tpch_conf

delta_mode=decay
bd_prob=0.9
budget=1500

#for budget in 0 50 100 150 200 250 300 350 400 450 500 550 600 
#do
    let budget=budget*1024

    for query in q3 q7 q8 q10
    do
           psql \
             -X \
             -U totemtang \
             -d tpch \
             -v v_budget=${budget} \
	         -v v_mode=${delta_mode} \
             -v v_prob=${bd_prob} \
             -f $BENCH_HOME/iqp_query/${query}_para.sql \
             > /dev/null
    done
#done

for query in q3 q7 q8 q10
do
           psql \
             -X \
             -U totemtang \
             -d tpch \
             -v v_mode=${delta_mode} \
             -v v_prob=${bd_prob} \
             -f $BENCH_HOME/dbt_query/${query}_para.sql \
             > /dev/null
done


