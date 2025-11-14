SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP TABLE IF exists dev.[dbo].[usgs_earthquake_data]

CREATE TABLE [dbo].[usgs_earthquake_data](
	[id] [nvarchar](50),
	[longitude] [float],
	[latitude] [float],
	[elevation] [float],
	[title] [nvarchar](255),
	[place_description] [nvarchar](255),
	[sig] [smallint],
	[mag] [float],
	[magType] [nvarchar](50),
	[time] [datetime2](7),
	[updated] [datetime2](7),
	[mag_class] [nvarchar](50),
	[country_code] [nvarchar](50),
	[earthquake_type] [nvarchar](50)
) ON [PRIMARY]
GO


