#!/bin/bash
## script to loop over airflow backfill command
# you can ue the timeout 3s command before airflow command
# to kill the process in order to only schedule the job and not running
# eg: timeout 3s airflow backfill -s $currentdate -e 2020-12-17 dccnl_xandr_programmatic

datestart="2020-10-07"
dateend="2020-12-18"

currentdate="$datestart"
while [[ "$currentdate" != "$dateend" ]]; do
    echo "backfilling for $currentdate"
    
    airflow backfill -s $currentdate -e 2020-12-17 dccnl_xandr_programmatic
    # currentdate = tomorrow
    currentdate="$(date -u --date="$currentdate tomorrow" '+%Y-%m-%d')"
    
    echo "################################# $currentdate done #####################################"
    
done