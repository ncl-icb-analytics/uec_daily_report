--Statement to create the table for the tracking tables
CREATE TABLE [Data_Lab_NCL_Dev].[JakeK].[uec_tracker_p2_test] (
	date_extract DATE NOT NULL,
	date_data DATE NOT NULL,
	provider CHAR(20) NOT NULL,
	unit CHAR(40),
	beds_available INT NOT NULL,
	beds_occupied INT NOT NULL

	PRIMARY KEY (date_data, unit)
)