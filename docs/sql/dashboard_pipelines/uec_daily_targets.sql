SELECT 
	tar.dataset,
	tar.metric_name,
	tar.site,
	YEAR(DATEADD(MONTH, n.number, active_from)) AS target_year,
	MONTH(DATEADD(MONTH, n.number, active_from)) AS target_month,
	tar.rag_green,
	tar.rag_amber,
	tar.target_direction
FROM (
	SELECT
		[dataset],
		[metric_name],
		[site],
		[active_from],
		[active_until],
		[target_value] as [rag_green],
		CASE
			WHEN target_direction = 'pos' THEN 
				CASE
					WHEN target_lenience_type = 'abs' THEN [target_value] - [target_lenience_amount]
					WHEN target_lenience_type = 'rel' THEN [target_value] - ([target_value] * [target_lenience_amount])
				END
			WHEN target_direction = 'neg' THEN
				CASE
					WHEN target_lenience_type = 'abs' THEN [target_value] + [target_lenience_amount]
					WHEN target_lenience_type = 'rel' THEN [target_value] + ([target_value] * [target_lenience_amount])
				END
		END AS [rag_amber],
		[target_direction]

	  FROM [Data_Lab_NCL_Dev].[JakeK].[uec_daily_targets]
	  WHERE (active_until IS NULL OR DATEADD(d, -60, GETDATE()) <= active_until)
) tar

LEFT JOIN master.dbo.spt_values n
ON n.type = 'P'
AND n.number <= 12

WHERE 
(DATEADD(MONTH, n.number, active_from) < active_until OR active_until IS NULL)

--WARNING! Do not remove the condition below or the query will return infinited rows!--
AND DATEADD(MONTH, n.number - 1, active_from) < GETDATE()