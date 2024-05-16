SELECT 
	date_data,
	MONTH(date_data) AS month_data,
	g_acute_sites as shorthand,
	ae_attendances,
	extended_patients_occupied,
	[g&acorebedsopen],
	number_of_discharges,
	over12hoursfromarrival

FROM [Data_Lab_NCL_Dev].[JakeK].[uec_daily_ecist] uec

LEFT JOIN [Data_Lab_NCL_Dev].[JakeK].[orgs] org
ON org.site_code = uec.provider_code
AND g_acute_sites IS NOT NULL

WHERE org.g_acute_sites != 'CFH'
AND date_data >= DATEADD(d, -40, GETDATE())