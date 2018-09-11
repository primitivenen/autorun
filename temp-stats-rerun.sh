days="
20171017
20171018
20180523
20180524
20180525
20180526
20180527
20180528
20180529
20180530
20180601
20180602
20180603
20180604
"

for day in $days
do
   echo $day
   python guobiao_daily_run.py -d $day --force-run |& tee -a guobiao_stats/${day}/log.txt
   python guobiao_daily_run.py -d $day --freq hourly --force-run |& tee -a guobiao_stats/${day}/log.txt
done

