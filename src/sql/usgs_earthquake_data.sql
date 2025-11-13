SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[usgs_earthquake_data](
	[id] [nvarchar](50) NOT NULL,
	[longitude] [float] NOT NULL,
	[latitude] [float] NOT NULL,
	[elevation] [float] NOT NULL,
	[title] [nvarchar](100) NOT NULL,
	[place_description] [nvarchar](50) NOT NULL,
	[sig] [smallint] NOT NULL,
	[mag] [float] NOT NULL,
	[magType] [nvarchar](50) NOT NULL,
	[time] [datetime2](7) NOT NULL,
	[updated] [datetime2](7) NOT NULL,
	[sig_class] [nvarchar](50) NOT NULL,
	[country_code] [nvarchar](50) NOT NULL
) ON [PRIMARY]
GO


