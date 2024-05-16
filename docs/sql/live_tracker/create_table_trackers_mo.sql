--Statement to create the table for the tracking tables
CREATE TABLE [Data_Lab_NCL_Dev].[JakeK].[uec_tracker_mo_test] (
	date_extract DATE NOT NULL,
	date_data DATE NOT NULL,
	provider CHAR(20) NOT NULL,
	metric_name CHAR(20) NOT NULL,
	metric_value INT NOT NULL

	PRIMARY KEY (date_data, provider, metric_name)
)