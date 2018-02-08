#!/bin/bash

BENCH_HOME=/home/totemtang/IQP/postgresql/pg_scripts/tpch_test

INC=off
dm=topdown

for query in q3
do
   for dm in dp
   do 
       for budget in 0
       do
           let budget=budget*1024
           psql \
             -X \
             -U totemtang \
             -d tpch \
             -v v_inc=${INC} \
             -v v_budget=${budget} \
             -v v_dm=${dm} \
             -f $BENCH_HOME/$query.sql

       done
   done
done


