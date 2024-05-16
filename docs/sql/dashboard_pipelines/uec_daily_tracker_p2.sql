SELECT 
	date_data,
	MONTH(date_data) AS month_data,
	SUM(beds_occupied) AS beds_occupied,
	SUM(beds_available) AS beds_available,
	CAST(SUM(beds_occupied) AS float) / SUM(beds_available) AS [p2_occupancy_%]
FROM [Data_Lab_NCL_Dev].[JakeK].[uec_daily_tracker_p2]
WHERE date_data >= DATEADD(d, -40, GETDATE())

GROUP BY date_data