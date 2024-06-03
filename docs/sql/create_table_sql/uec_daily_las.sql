CREATE TABLE [Data_Lab_NCL_Dev].[JakeK].[uec_daily_las] (
	[date_data] DATE NOT NULL,
    [provider_code] CHAR(5) NOT NULL,
    [total_handover] BIGINT, 
    [over_15mins] BIGINT,
    [over_30mins] BIGINT,
    [over_45mins] BIGINT,
    [over_60mins] BIGINT,
    [over_120mins] BIGINT,
    [over_180mins] BIGINT
)