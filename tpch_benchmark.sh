#!/bin/bash

BENCH_HOME=/home/totemtang/IQP/postgresql/pg_scripts/tpch_test
DELTA_HOME=/home/totemtang/IQP/postgresql/pg_scripts/tpch_delta

INC=off
dm=dp
update=lineitem

for query in q7
do
   for update in orders,lineitem
   do 
       for budget in 1500
       do
           if [ "$INC" == "on" ]
           then
               IFS=',' read -r -a update_array <<< "${update}"
               for oneupdate in "${update_array[@]}"
               do
                   $DELTA_HOME/delete_delta.sh $oneupdate
               done
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


