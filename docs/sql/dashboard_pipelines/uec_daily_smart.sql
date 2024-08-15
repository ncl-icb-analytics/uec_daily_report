SELECT * 
FROM (
	SELECT 
		ReportDate as date_data,
		MONTH(ReportDate) AS month_data,
		CASE	
			WHEN siteName = 'Barnet Hospital' THEN 'BH'
			WHEN siteName = 'Royal Free Hospital' THEN 'RFH'
			WHEN siteName = 'Chase Farm HospitaL' THEN 'CFH'
			WHEN siteName = 'North Middlesex Hospital' THEN 'NMUH'
			WHEN siteName = 'UCLH' THEN 'UCLH'
			WHEN siteName = 'Whittington Hospital' THEN 'WH'
			ELSE NULL
		END AS [shorthand],
		IndicatorKeyName AS [metric_name],
		Value_numeric as [metric_value]
	FROM [Data_Lab_NCL_Dev].[JakeK].[uec_daily_smart_vw]

	WHERE [reportDate] >= GETDATE()-40
	AND IndicatorKeyName IN ('breaches','no_of_attendances','dta_in_ed_at_time_of_reporting','medically_optimised', 'opel_level')

	--Filter out CFH DTA and MO data as they are not reported in the dashboard
	AND (siteName != 'Chase Farm Hospital' OR IndicatorKeyName NOT IN ('dta_in_ed_at_time_of_reporting','medically_optimised'))
) src

PIVOT (
	SUM(metric_value)
	FOR metric_name IN
		([breaches], [no_of_attendances], [dta_in_ed_at_time_of_reporting], [medically_optimised], [opel_level])
) AS pvt