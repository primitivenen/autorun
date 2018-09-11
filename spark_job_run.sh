# use today as ending date, X days ago as starting date
# X is a passed in argument
cd /home/stang/user-profile/stats-spark

end_date=`date --date="today" +%Y%m%d`
start_date=`date --date="$1 day ago" +%Y%m%d`

#start_date=20180521
#end_date=20180602

d=$start_date
while [[ "$d" -le $end_date ]]; do
    echo $d

    logpath=ag_stats/$d/
    mkdir -p $logpath

    # calculate daily stats
    echo `date` |& tee -a $logpath/log.txt
    python daily_run.py -d $d   |& tee -a $logpath/log.txt
    echo "======================================" |& tee -a $logpath/log.txt

    # calculate hourly stats
    echo `date` |& tee -a $logpath/log.txt
    python daily_run.py -d $d --freq hourly     |& tee -a $logpath/log.txt
    echo "======================================" |& tee -a $logpath/log.txt

    # calcualte trip stats
    triplog=trip_stats/log.txt
    echo `date` |& tee -a $triplog
    python trips_run.py -d $d        |& tee -a $triplog
    echo "======================================" |& tee -a $triplog

    d=$(date --date="$d + 1 day" +%Y%m%d)
done

