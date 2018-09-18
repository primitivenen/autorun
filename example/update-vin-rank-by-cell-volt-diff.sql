CREATE TABLE guobiao_tsp_tbls.vin_rank_by_cell_volt_diff AS
SELECT vin, ROW_NUMBER() OVER (ORDER BY mean_daily_avg_cell_volt_diff DESC) AS row_number,
mean_daily_avg_cell_volt_diff,
FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd') AS update_date,
TO_DATE(LAST_DAY_STATS) AS last_daily_stats_day
FROM 
(
SELECT vin, MAX(day) AS last_day, COUNT(day) AS counts, AVG(cell_volt_diff_mean) AS mean_daily_avg_cell_volt_diff
FROM
(
	SELECT vin, day, cell_volt_diff_mean
       	FROM guobiao_tsp_tbls.daily_stats 
	WHERE
	vin IN (SELECT vin FROM guobiao_tsp_tbls.vintypes WHERE vintype = 'A5HEV')
       	AND day > date_sub(LAST_DAY_STATS, 30)
) a
GROUP BY vin
HAVING counts > 5
) b
