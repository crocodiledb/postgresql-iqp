#!/bin/bash

BENCH_HOME=/home/totemtang/IQP/postgresql/pg_scripts/tpch_test
DELTA_HOME=/home/totemtang/IQP/postgresql/pg_scripts/tpch_delta

INC=on
dm=dp
update=customer

for query in q8
do
   for update in supplier
   do 
       for budget in 150
       do
           if [ "$INC" == "on" ]
           then
               $DELTA_HOME/delete_delta.sh $update
           fi

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


