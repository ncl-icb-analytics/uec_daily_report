SELECT 
	date_data,
	MONTH(date_data) AS month_data,
	SUM(capacity) AS capacity,
	SUM(occupied) AS occupied,
	CAST(SUM(occupied) AS float) / SUM(capacity) AS [vw_occupancy_%]

FROM [Data_Lab_NCL_Dev].[JakeK].[uec_daily_tracker_vw]
WHERE date_data >= DATEADD(d, -40, GETDATE())

GROUP BY date_data