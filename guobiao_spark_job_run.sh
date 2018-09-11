# use today as ending date, X days ago as starting date
# X is a passed in argument
cd /home/stang/new_data_sources

end_date=`date --date="today" +%Y%m%d`
start_date=`date --date="$1 day ago" +%Y%m%d`

#start_date=20180523
#end_date=20180602

d=$start_date
while [[ "$d" -le $end_date ]]; do
    echo $d

    logpath=guobiao_stats/$d/
    mkdir -p $logpath

    # calculate daily stats
    echo `date` |& tee -a $logpath/log.txt
    python guobiao_daily_run.py -d $d     |& tee -a $logpath/log.txt
    echo "======================================" |& tee -a $logpath/log.txt

    # calculate hourly stats
    echo `date` |& tee -a $logpath/log.txt
    python guobiao_daily_run.py -d $d --freq hourly        |& tee -a $logpath/log.txt
    echo "======================================" |& tee -a $logpath/log.txt

    d=$(date --date="$d + 1 day" +%Y%m%d)
done

