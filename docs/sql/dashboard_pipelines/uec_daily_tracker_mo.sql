SELECT 
	date_data,
	MONTH(date_data) AS month_data,
	provider,
	metric_name,
	metric_value
FROM [Data_Lab_NCL_Dev].[JakeK].[uec_daily_tracker_mo]
WHERE date_data >= DATEADD(d, -40, GETDATE())