--Script to check for data quality issues in the source files
--(Most commonly this is someone entering the wrong date into the file)
SELECT TOP(6) * FROM [Data_Lab_NCL_Dev].[JakeK].[uec_daily_tracker_mo] ORDER BY date_data DESC;

SELECT TOP(6) * FROM [Data_Lab_NCL_Dev].[JakeK].[uec_daily_tracker_p2] ORDER BY date_data DESC;

SELECT TOP(4) * FROM [Data_Lab_NCL_Dev].[JakeK].[uec_daily_tracker_vw] ORDER BY date_data DESC;