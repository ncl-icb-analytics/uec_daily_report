--Statement to create the table for the tracking tables
CREATE TABLE [Data_Lab_NCL_Dev].[JakeK].[uec_daily_tracker_mo] (
	date_extract DATE NOT NULL,
	date_data DATE NOT NULL,
	provider_code CHAR(5) NOT NULL,
	metric_name CHAR(20) NOT NULL,
	metric_value INT NOT NULL

	PRIMARY KEY (date_data, provider_code, metric_name)
)