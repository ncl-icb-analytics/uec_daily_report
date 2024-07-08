--Script to pull UDAL data into the UEC Daily Dashboard
SELECT
	[date_data],
	MONTH([date_data]) AS month_data,
	[shorthand],
	[ae_attendances],
	[extended_patients_occupied],
	[g&acorebedsopen],
	[number_of_discharges],
	[over12hoursfromarrival]

--Sub-query to fetch unpivoted UDAL data
FROM (
	SELECT 
		[Period] AS [date_data],
		org.[g_acute_sites] as [shorthand],
		[MetricName],
		[Value]

	FROM [Data_Store_UDAL].[UDAL].[UDAL_Daily_sitrep] uec

	LEFT JOIN [Data_Lab_NCL_Dev].[JakeK].[orgs] org
	ON org.site_code = uec.[Site_Code]
	AND g_acute_sites IS NOT NULL
  
	WHERE org.g_acute_sites != 'CFH'
	AND [Period] >= DATEADD(d, -40, GETDATE())
	AND [MetricName] IN (
		'AE_Attendances',
		'Extended_Patients_Occupied',
		'G&ACoreBedsOpen',
		'Number_of_Discharges',
		'Over12HoursFromArrival')
) udal

--Pivot the UDAL data
PIVOT (
	SUM([Value])
	FOR [MetricName] IN (
		[AE_Attendances],
		[Extended_Patients_Occupied],
		[G&ACoreBedsOpen],
		[Number_of_Discharges],
		[Over12HoursFromArrival])
) AS pvt