CREATE TABLE [Data_Lab_NCL_Dev].[JakeK].[uec_daily_ecist_test] (
	[date_data] DATE NOT NULL,
    [provider_code] CHAR(5) NOT NULL,
	[accbedsoccupied] BIGINT, 
	[accbedsopen] BIGINT, 
    [ae_attendances] BIGINT,
	[beds_occupied_by_long_stay_patients_14_plus_days] BIGINT,
    [extended_patients_occupied] BIGINT,
    [g&acorebedsopen] BIGINT,
	[g&atotalbedsoccupied] BIGINT,
    [number_of_discharges] BIGINT,
    [over12hoursfromarrival] BIGINT,
	[unavailablebedstrandedpatients] BIGINT
)