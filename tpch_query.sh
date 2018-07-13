#!/bin/bash

BENCH_HOME=/home/totemtang/IQP/postgresql/tpch_conf

delta_mode=uniform
bd_prob=0.9
budget=1500

for budget in 800  
do
    let budget=budget*1024

    for query in q9
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
done

#for query in q3 q7 q8 q10
#do
#           psql \
#             -X \
#             -U totemtang \
#             -d tpch \
#             -v v_mode=${delta_mode} \
#             -v v_prob=${bd_prob} \
#             -f $BENCH_HOME/dbt_query/${query}_para.sql \
#             > /dev/null
#done


