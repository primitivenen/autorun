# use today as ending date, X days ago as starting date
# X is a passed in argument
cd /home/trangel/history-stats/

end_date=`date --date="today" +%Y%m%d`
start_date=`date --date="$1 day ago" +%Y%m%d`

echo $end_date $start_date
#start_date=20180510
#end_date=20180510

d=$start_date
while [[ "$d" -le $end_date ]]; do
    echo $d

    logpath=log_files
    mkdir -p $logpath

    # calculate daily stats
    echo `date` |& tee -a $logpath/log-${d}.txt
    python battery_spark.py $d |& tee -a $logpath/log-${d}.txt
    echo "======================================" |& tee -a $logpath/log-${d}.txt

    # export to hive
    python battery_export.py $d |& tee -a $logpath/log-${d}.txt
    echo "======================================" |& tee -a $logpath/log-${d}.txt

    d=$(date --date="$d + 1 day" +%Y%m%d)
done
