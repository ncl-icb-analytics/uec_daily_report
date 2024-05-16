SELECT 
	date_data,
	MONTH(date_data) AS month_data,
	org.g_acute_sites AS shorthand,
	metric_name,
	metric_value
FROM [Data_Lab_NCL_Dev].[JakeK].[uec_daily_tracker_mo] mo

LEFT JOIN [Data_Lab_NCL_Dev].[JakeK].[orgs] org
ON org.site_code = mo.provider_code
AND g_acute_sites IS NOT NULL

WHERE date_data >= DATEADD(d, -40, GETDATE())
AND g_acute_sites != 'CFH'