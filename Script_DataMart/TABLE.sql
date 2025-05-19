USE [ran5g]
GO

/****** Object:  Table [dbo].[DIM_DATE_TIME]    Script Date: 4/19/2025 10:20:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[DIM_DATE_TIME](
	[DATETIME_ID] [int] NOT NULL,
	[DATETIME_NAME] [varchar](16) NULL,
	[DATETIME_ALTERNATE] [datetime2](0) NULL,
	[DATETIME_HOUR_OF_DAY] [varchar](2) NULL,
	[DATE_ID] [int] NULL,
	[DATE_NAME] [varchar](10) NOT NULL,
	[DATE_DAY_OF_WEEK_LONG_NAME] [varchar](9) NULL,
	[DATE_DAY_OF_WEEK_SHORT_NAME] [varchar](3) NULL,
	[DATE_DAY_NUM_OF_WEEK] [int] NULL,
	[DATEW_DAY_NUM_OF_WEEK] [int] NULL,
	[DATE_DAY_NUM_OF_MONTH] [int] NULL,
	[DATE_DAY_NUM_OF_YEAR] [int] NULL,
	[WEEK_ID] [int] NULL,
	[WEEK_NAME] [varchar](46) NULL,
	[WEEK_NUM_OF_YEAR] [int] NULL,
	[WEEK_START_DATE] [varchar](10) NULL,
	[WEEK_END_DATE] [varchar](10) NULL,
	[MONTH_ID] [int] NULL,
	[MONTH_NUMBER_OF_DAY] [int] NULL,
	[MONTH_START_DATE] [varchar](10) NULL,
	[MONTH_END_DATE] [varchar](10) NULL,
	[MONTH_SHORT_NAME] [varchar](3) NULL,
	[MONTH_LONG_NAME] [varchar](9) NULL,
	[MONTH_YEAR_SHORT_NAME] [varchar](8) NULL,
	[MONTH_YEAR_LONG_NAME] [varchar](14) NULL,
	[MONTH_NUM_OF_YEAR] [int] NULL,
	[QUARTER_ID] [varchar](7) NULL,
	[QUARTER_START_DATE] [varchar](10) NULL,
	[QUARTER_END_DATE] [varchar](10) NULL,
	[QUARTER_NUM_OF_YEAR] [int] NULL,
	[ISO_YEAR_ID] [varchar](4) NULL,
	[YEAR_ID] [varchar](4) NULL,
	[YEAR_NUMBER_OF_DAY] [int] NULL,
 CONSTRAINT [PK_DATETIME_ID] PRIMARY KEY CLUSTERED 
(
	[DATETIME_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[DIM_DATE_TIME_TEMP]    Script Date: 4/19/2025 10:20:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[DIM_DATE_TIME_TEMP](
	[Date Time.Month] [nvarchar](10) NULL,
	[Date Time.Quarter] [nvarchar](10) NULL,
	[Date Time.Year] [nvarchar](10) NULL,
	[Date Time.Date name] [nvarchar](50) NULL,
	[Date Time.Date Number In Month] [nvarchar](10) NULL,
	[Date Time.Week Number In Year] [nvarchar](10) NULL,
	[Date Time.Week] [nvarchar](50) NULL,
	[Date Time.QUARTER ID] [nvarchar](10) NULL
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[DIM_DVT]    Script Date: 4/19/2025 10:20:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[DIM_DVT](
	[DVT] [nvarchar](4000) NULL,
	[TTML] [nvarchar](4000) NULL
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[DIM_MONTH]    Script Date: 4/19/2025 10:20:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[DIM_MONTH](
	[Date Time.Month] [nvarchar](10) NULL,
	[Date Time.Quarter] [nvarchar](10) NULL,
	[Date Time.Year] [nvarchar](10) NULL
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[DIM_QUARTER]    Script Date: 4/19/2025 10:20:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[DIM_QUARTER](
	[QUARTER_NUM] [nvarchar](10) NULL,
	[YEAR] [nvarchar](10) NULL,
	[QUARTERID] [nvarchar](10) NULL
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[DIM_TERRITORY]    Script Date: 4/19/2025 10:20:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[DIM_TERRITORY](
	[DISTRICT] [nvarchar](4000) NULL,
	[PROVINCE] [nvarchar](4000) NULL,
	[TTML] [nvarchar](4000) NULL
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[DIM_VENDOR]    Script Date: 4/19/2025 10:20:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[DIM_VENDOR](
	[VENDOR] [nvarchar](100) NULL
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[DIM_WEEK]    Script Date: 4/19/2025 10:20:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[DIM_WEEK](
	[WEEK_NUM] [nvarchar](10) NULL,
	[YEAR] [nvarchar](10) NULL,
	[WEEKID] [nvarchar](50) NULL
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[DIM_YEAR]    Script Date: 4/19/2025 10:20:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[DIM_YEAR](
	[Date Time.Year] [nvarchar](10) NULL
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[DimRan5G]    Script Date: 4/19/2025 10:20:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[DimRan5G](
	[CELL_NODE_ID] [int] NULL,
	[G_CELLID] [nvarchar](100) NULL,
	[G_CELLNAME] [nvarchar](100) NULL,
	[DISTRICT] [nvarchar](4000) NULL,
	[G_NODEID] [nvarchar](100) NULL,
	[G_NODENAME] [nvarchar](100) NULL,
	[VENDOR] [nvarchar](100) NULL,
	[PROVINCE] [nvarchar](4000) NULL,
	[SERVICE_REGION] [nvarchar](255) NULL,
	[NOC] [nvarchar](4000) NULL,
	[TELCOSUPPORTORG] [nvarchar](4000) NULL,
	[TELCOSUPPORTGROUP] [nvarchar](4000) NULL,
	[NW_REGION] [nvarchar](4000) NULL,
	[TYPE] [nvarchar](4000) NULL,
	[BRANCH] [nvarchar](4000) NULL,
	[NOC_ORDER] [nvarchar](50) NULL,
	[NW_REGION_ORDER] [nvarchar](50) NULL,
	[DISTRICTID] [int] NULL,
	[TVTID] [int] NULL
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[MART_RAN5G_REPORT_DAILY]    Script Date: 4/19/2025 10:20:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[MART_RAN5G_REPORT_DAILY](
	[DATETIME_ID] [int] NULL,
	[Date_Number_In_Month] [nvarchar](50) NULL,
	[Month] [nvarchar](50) NULL,
	[Quarter] [nvarchar](50) NULL,
	[Year] [nvarchar](50) NULL,
	[Week_Number_In_Year] [nvarchar](50) NULL,
	[District] [nvarchar](50) NULL,
	[Province] [nvarchar](50) NULL,
	[Chi_Nhanh] [nvarchar](50) NULL,
	[CTKD] [nvarchar](50) NULL,
	[VENDOR] [nvarchar](50) NULL,
	[TVT] [nvarchar](50) NULL,
	[DVT] [nvarchar](50) NULL,
	[TTML] [nvarchar](50) NULL,
	[PS_CSSR_NR] [float] NULL,
	[SDR_NR] [float] NULL,
	[PRB_Util_DL_NR] [float] NULL,
	[PRB_Util_UL_NR] [float] NULL,
	[Latency_NR] [float] NULL,
	[PktLossR] [float] NULL,
	[Connected_RRC_user_average] [float] NULL,
	[Connected_RRC_user_Max] [float] NULL,
	[DKD5G_NR] [float] NULL,
	[EN_DC_SR_NR] [float] NULL,
	[HOSR_NR] [float] NULL,
	[RASR_NR] [float] NULL,
	[DL_TRAFFIC_NR] [float] NULL,
	[UL_TRAFFIC_NR] [float] NULL,
	[User_DL_THP_NR] [float] NULL,
	[User_UL_THP_NR] [float] NULL,
	[Cell_DL_THP_NR] [float] NULL,
	[Cell_UL_THP_NR] [float] NULL,
	[LEVEL] [varchar](8) NOT NULL
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[MART_RAN5G_REPORT_MONTHLY]    Script Date: 4/19/2025 10:20:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[MART_RAN5G_REPORT_MONTHLY](
	[MONTH_ID] [int] NOT NULL,
	[Month] [nvarchar](50) NULL,
	[Quarter] [nvarchar](50) NULL,
	[Year] [nvarchar](50) NULL,
	[District] [nvarchar](50) NULL,
	[Province] [nvarchar](50) NULL,
	[Chi_Nhanh] [nvarchar](50) NULL,
	[CTKD] [nvarchar](50) NULL,
	[VENDOR] [nvarchar](50) NULL,
	[TVT] [nvarchar](50) NULL,
	[DVT] [nvarchar](50) NULL,
	[TTML] [nvarchar](50) NULL,
	[PS_CSSR_NR] [float] NULL,
	[SDR_NR] [float] NULL,
	[PRB_Util_DL_NR] [float] NULL,
	[PRB_Util_UL_NR] [float] NULL,
	[Latency_NR] [float] NULL,
	[PktLossR] [float] NULL,
	[Connected_RRC_user_average] [float] NULL,
	[Connected_RRC_user_Max] [float] NULL,
	[DKD5G_NR] [float] NULL,
	[EN_DC_SR_NR] [float] NULL,
	[HOSR_NR] [float] NULL,
	[RASR_NR] [float] NULL,
	[DL_TRAFFIC_NR] [float] NULL,
	[UL_TRAFFIC_NR] [float] NULL,
	[User_DL_THP_NR] [float] NULL,
	[User_UL_THP_NR] [float] NULL,
	[Cell_DL_THP_NR] [float] NULL,
	[Cell_UL_THP_NR] [float] NULL,
	[LEVEL] [varchar](8) NOT NULL
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[MART_RAN5G_REPORT_QUARTERLY]    Script Date: 4/19/2025 10:20:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[MART_RAN5G_REPORT_QUARTERLY](
	[Quarter] [nvarchar](50) NULL,
	[Year] [nvarchar](50) NULL,
	[District] [nvarchar](50) NULL,
	[Province] [nvarchar](50) NULL,
	[Chi_Nhanh] [nvarchar](50) NULL,
	[CTKD] [nvarchar](50) NULL,
	[VENDOR] [nvarchar](50) NULL,
	[TVT] [nvarchar](50) NULL,
	[DVT] [nvarchar](50) NULL,
	[TTML] [nvarchar](50) NULL,
	[PS_CSSR_NR] [float] NULL,
	[SDR_NR] [float] NULL,
	[PRB_Util_DL_NR] [float] NULL,
	[PRB_Util_UL_NR] [float] NULL,
	[Latency_NR] [float] NULL,
	[PktLossR] [float] NULL,
	[Connected_RRC_user_average] [float] NULL,
	[Connected_RRC_user_Max] [float] NULL,
	[DKD5G_NR] [float] NULL,
	[EN_DC_SR_NR] [float] NULL,
	[HOSR_NR] [float] NULL,
	[RASR_NR] [float] NULL,
	[DL_TRAFFIC_NR] [float] NULL,
	[UL_TRAFFIC_NR] [float] NULL,
	[User_DL_THP_NR] [float] NULL,
	[User_UL_THP_NR] [float] NULL,
	[Cell_DL_THP_NR] [float] NULL,
	[Cell_UL_THP_NR] [float] NULL,
	[LEVEL] [varchar](8) NOT NULL
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[MART_RAN5G_REPORT_WEEKLY]    Script Date: 4/19/2025 10:20:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[MART_RAN5G_REPORT_WEEKLY](
	[WEEK_ID] [int] NOT NULL,
	[Week] [varchar](2) NULL,
	[Year] [varchar](4) NULL,
	[District] [nvarchar](50) NULL,
	[Province] [nvarchar](50) NULL,
	[Chi_Nhanh] [nvarchar](50) NULL,
	[CTKD] [nvarchar](50) NULL,
	[VENDOR] [nvarchar](50) NULL,
	[TVT] [nvarchar](50) NULL,
	[DVT] [nvarchar](50) NULL,
	[TTML] [nvarchar](50) NULL,
	[PS_CSSR_NR] [float] NULL,
	[SDR_NR] [float] NULL,
	[PRB_Util_DL_NR] [float] NULL,
	[PRB_Util_UL_NR] [float] NULL,
	[Latency_NR] [float] NULL,
	[PktLossR] [float] NULL,
	[Connected_RRC_user_average] [float] NULL,
	[Connected_RRC_user_Max] [float] NULL,
	[DKD5G_NR] [float] NULL,
	[EN_DC_SR_NR] [float] NULL,
	[HOSR_NR] [float] NULL,
	[RASR_NR] [float] NULL,
	[DL_TRAFFIC_NR] [float] NULL,
	[UL_TRAFFIC_NR] [float] NULL,
	[User_DL_THP_NR] [float] NULL,
	[User_UL_THP_NR] [float] NULL,
	[Cell_DL_THP_NR] [float] NULL,
	[Cell_UL_THP_NR] [float] NULL,
	[LEVEL] [varchar](8) NOT NULL
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[MART_RAN5G_REPORT_YEARLY]    Script Date: 4/19/2025 10:20:00 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[MART_RAN5G_REPORT_YEARLY](
	[Year] [nvarchar](50) NULL,
	[District] [nvarchar](50) NULL,
	[Province] [nvarchar](50) NULL,
	[Chi_Nhanh] [nvarchar](50) NULL,
	[CTKD] [nvarchar](50) NULL,
	[VENDOR] [nvarchar](50) NULL,
	[TVT] [nvarchar](50) NULL,
	[DVT] [nvarchar](50) NULL,
	[TTML] [nvarchar](50) NULL,
	[PS_CSSR_NR] [float] NULL,
	[SDR_NR] [float] NULL,
	[PRB_Util_DL_NR] [float] NULL,
	[PRB_Util_UL_NR] [float] NULL,
	[Latency_NR] [float] NULL,
	[PktLossR] [float] NULL,
	[Connected_RRC_user_average] [float] NULL,
	[Connected_RRC_user_Max] [float] NULL,
	[DKD5G_NR] [float] NULL,
	[EN_DC_SR_NR] [float] NULL,
	[HOSR_NR] [float] NULL,
	[RASR_NR] [float] NULL,
	[DL_TRAFFIC_NR] [float] NULL,
	[UL_TRAFFIC_NR] [float] NULL,
	[User_DL_THP_NR] [float] NULL,
	[User_UL_THP_NR] [float] NULL,
	[Cell_DL_THP_NR] [float] NULL,
	[Cell_UL_THP_NR] [float] NULL,
	[LEVEL] [varchar](8) NOT NULL
) ON [PRIMARY]
GO
