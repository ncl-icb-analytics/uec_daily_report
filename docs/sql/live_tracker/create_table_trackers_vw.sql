--Statement to create the table for the tracking tables
CREATE TABLE [Data_Lab_NCL_Dev].[JakeK].[uec_tracker_vw_test] (
	date_data DATE NOT NULL,
	capacity INT,
	occupied INT,
	system_value FLOAT NOT NULL,
	includes_paediatric TINYINT NOT NULL

	PRIMARY KEY (date_data)
)
