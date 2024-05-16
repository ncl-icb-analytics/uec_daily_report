SELECT 
	date_data,
	MONTH(date_data) as month_data,
		g_acute_sites as shorthand, 
	over_120mins, 
	over_45mins, 
	total_handover
FROM [Data_Lab_NCL_Dev].[JakeK].[uec_daily_las] uec

LEFT JOIN [Data_Lab_NCL_Dev].[JakeK].[orgs] org
ON org.site_code = uec.provider_code
AND g_acute_sites IS NOT NULL

WHERE date_data >= DATEADD(d, -40, GETDATE())