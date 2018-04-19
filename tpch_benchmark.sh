#!/bin/bash

BENCH_HOME=/home/totemtang/IQP/postgresql/pg_scripts/tpch_test
DELTA_HOME=/home/totemtang/IQP/postgresql/pg_scripts/tpch_delta

INC=off
dm=dp
update=lineitem

for query in q9
do
   for update in orders,supplier
   do 
       for budget in 1500
       do
           #if [ "$INC" == "on" ]
           #then
           #    $DELTA_HOME/delete_delta.sh $update
           #fi

           let budget=budget*1024
           psql \
             -X \
             -U totemtang \
             -d tpch \
             -v v_inc=${INC} \
             -v v_budget=${budget} \
             -v v_dm=${dm} \
             -v v_update="'${update}'" \
             -f $BENCH_HOME/$query.sql
       done
   done
done


