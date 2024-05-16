--Statement to create the table for the tracking tables
CREATE TABLE [Data_Lab_NCL_Dev].[JakeK].[uec_daily_tracker_vw] (
	date_data DATE NOT NULL,
	capacity INT,
	occupied INT,
	system_value FLOAT NOT NULL,
	includes_paediatric TINYINT NOT NULL

	PRIMARY KEY (date_data)
)
