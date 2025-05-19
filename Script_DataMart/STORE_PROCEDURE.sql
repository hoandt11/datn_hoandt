USE [ran5g]
GO

/****** Object:  StoredProcedure [dbo].[InsertDimDateTimeTemp]    Script Date: 5/19/2025 10:23:17 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[InsertDimDateTimeTemp]
AS
BEGIN
    DECLARE @StartDatetimeID INT = 2024112700;  -- Ngày bắt đầu là 27/11/2024 theo định dạng YYYYMMDDHH

    -- Lấy ngày tối đa từ bảng Mart daily
    DECLARE @MaxDateID INT = (SELECT MAX(DATETIME_ID) FROM [MART_RAN5G_REPORT_DAILY]);

    -- Chèn dữ liệu từ StartDatetimeID đến MaxDateID vào bảng DIM_DATE_TIME_TEMP
    IF @MaxDateID > @StartDatetimeID
    BEGIN
        -- Chèn dữ liệu vào bảng DIM_DATE_TIME_TEMP
		TRUNCATE TABLE DIM_DATE_TIME_TEMP

        INSERT INTO DIM_DATE_TIME_TEMP 
        (
            [Date Time.Month],
            [Date Time.Quarter],
            [Date Time.Year],
            [Date Time.Date name],
            [Date Time.Date Number In Month],
            [Date Time.Week Number In Year],
            [Date Time.Week],
            [Date Time.QUARTER ID]
        )
        SELECT 
            CAST([MONTH_NUM_OF_YEAR] AS NVARCHAR(10)),  
			CAST([QUARTER_NUM_OF_YEAR] AS NVARCHAR(10)),
			CAST([YEAR_ID] AS NVARCHAR(10)),
			COUNT(CAST([DATE_NAME] AS NVARCHAR(50))) AS DateNameCount,  
			CAST([DATE_DAY_NUM_OF_MONTH] AS NVARCHAR(10)),
			CAST([WEEK_NUM_OF_YEAR] AS NVARCHAR(10)),
			CAST([WEEK_NAME] AS NVARCHAR(50)),
			CAST([QUARTER_ID] AS NVARCHAR(10))
        FROM [DIM_DATE_TIME] 
        WHERE DATETIME_ID BETWEEN @StartDatetimeID AND @MaxDateID
        GROUP BY 
			[MONTH_NUM_OF_YEAR], 
			[QUARTER_NUM_OF_YEAR], 
			[YEAR_ID], 
			[DATE_DAY_NUM_OF_MONTH], 
			[WEEK_NUM_OF_YEAR], 
			[WEEK_NAME], 
			[QUARTER_ID]
		ORDER BY [YEAR_ID];

    END
END;

GO

/****** Object:  StoredProcedure [dbo].[RAN_5G_DAY_GENERAL]    Script Date: 5/19/2025 10:23:17 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





CREATE PROCEDURE [dbo].[RAN_5G_DAY_GENERAL] 
	@ngay nvarchar(100) = '',
	@last int = 1
AS
BEGIN 
	SET NOCOUNT ON;
	DECLARE @tranform VARCHAR(max);
	DECLARE @mdx VARCHAR(max);

	SET @ngay = COALESCE(NULLIF(@ngay, ''), CONVERT(VARCHAR(10), GETDATE()-@last, 112));
	DECLARE @ngay_103 varchar(10) = CONVERT(VARCHAR(10), convert(datetime, @ngay),103);

		DROP TABLE IF EXISTS STAGING_RAN5G_DAY_DISTRICT;
		DROP TABLE IF EXISTS STAGING_RAN5G_DAY_PROVINCE;
		DROP TABLE IF EXISTS STAGING_RAN5G_DAY_BRANCH;
		DROP TABLE IF EXISTS STAGING_RAN5G_DAY_CTKD;
		DROP TABLE IF EXISTS STAGING_RAN5G_DAY_TTML;
		DROP TABLE IF EXISTS STAGING_RAN5G_DAY_VENDOR;
		DROP TABLE IF EXISTS STAGING_RAN5G_DAY_DVT;
		DROP TABLE IF EXISTS STAGING_RAN5G_DAY_TVT;
		DROP TABLE IF EXISTS STAGING_RAN5G_DAY_TOANMANG;


-----------------------DISTRICT -------------------------
EXEC('
    SELECT 
        CONVERT(int, SUBSTRING("[Measures].[DateCaption]", 7, 10) + SUBSTRING("[Measures].[DateCaption]", 4, 2) + SUBSTRING("[Measures].[DateCaption]", 1, 2) + ''00'') AS DATETIME_ID,
        CONVERT(NVARCHAR(50), "[Measures].[DateNumberCaption]") AS Date_Number_In_Month,
        CONVERT(NVARCHAR(50), "[Measures].[MonthCaption]") AS Month,
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CONVERT(NVARCHAR(50), "[Measures].[WeekNumberCaption]") AS Week_Number_In_Year,
        CONVERT(NVARCHAR(50), "[Measures].[DistrictCaption]") AS District,
        CONVERT(NVARCHAR(50), "[Measures].[ProvinceCaption]") AS Province,
        CONVERT(NVARCHAR(50), "[Measures].[BRANCHCaption]") AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
        CONVERT(NVARCHAR(50), "[Measures].[TVTCaption]") AS TVT,
        CONVERT(NVARCHAR(50), "[Measures].[DVTCaption]") AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''DISTRICT'' AS LEVEL
		INTO STAGING_RAN5G_DAY_DISTRICT
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[DateCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[DateNumberCaption] AS [Date Time].[Date Number In Month].CURRENTMEMBER.NAME
            MEMBER [Measures].[MonthCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.PARENT.NAME
            MEMBER [Measures].[WeekNumberCaption] AS [Date Time].[Week Number In Year].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
            MEMBER [Measures].[ProvinceCaption] AS [DimRan5G].[Province].CURRENTMEMBER.NAME
            MEMBER [Measures].[DistrictCaption] AS [DimRan5G].[District].CURRENTMEMBER.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
            MEMBER [Measures].[BRANCHCaption] AS [DimRan5G].[Chi Nhanh].CURRENTMEMBER.NAME
            MEMBER [Measures].[DVTCaption] AS [DimRan5G].[DVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[TVTCaption] AS [DimRan5G].[TVT].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[DateCaption],
                [Measures].[DateNumberCaption],
                [Measures].[MonthCaption],
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[WeekNumberCaption],
                [Measures].[TTMLCaption],
                [Measures].[ProvinceCaption],
                [Measures].[DistrictCaption],
                [Measures].[CTKDCaption],
                [Measures].[BRANCHCaption],
                [Measures].[DVTCaption],
                [Measures].[TVTCaption],
                KPIValue("PS_CSSR_NR"),
                KPIValue("SDR_NR"),
                KPIValue("PRB_Util_DL_NR"),
                KPIValue("PRB_Util_UL_NR"),
                KPIValue("Latency_NR"),
                KPIValue("PktLossR"),
                KPIValue("Connected_RRC_user_average"),
                KPIValue("Connected_RRC_user_Max"),
                KPIValue("DKD5G_NR"),
                KPIValue("EN_DC_SR_NR"),
                KPIValue("HOSR_NR"),
                KPIValue("RASR_NR"),
                KPIValue("DL_TRAFFIC_NR"),
                KPIValue("UL_TRAFFIC_NR"),
                KPIValue("User_DL_THP_NR"),
                KPIValue("User_UL_THP_NR"),
                KPIValue("Cell_DL_THP_NR"),
                KPIValue("Cell_UL_THP_NR")
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Date].&['+@ngay+'] : [Date Time].[Calendar].[Date].&['+@ngay+'] *
                [Date Time].[Date Number In Month].[Date Number In Month].ALLMEMBERS *
                [Date Time].[Week Number In Year].[Week Number In Year].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML] *
                [DimRan5G].[Province].[Province].ALLMEMBERS *
                [DimRan5G].[District].[District].ALLMEMBERS *
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS *
                [DimRan5G].[Chi Nhanh].[Chi Nhanh] *
                [DimRan5G].[DVT].[DVT] *
                [DimRan5G].[TVT].[TVT]
            } ON ROWS
        FROM ran5g'')
');

----------------- PROVINCE ----------------------------
EXEC('
    SELECT 
        CONVERT(int, SUBSTRING("[Measures].[DateCaption]", 7, 10) + SUBSTRING("[Measures].[DateCaption]", 4, 2) + SUBSTRING("[Measures].[DateCaption]", 1, 2) + ''00'') AS DATETIME_ID,
        CONVERT(NVARCHAR(50), "[Measures].[DateNumberCaption]") AS Date_Number_In_Month,
        CONVERT(NVARCHAR(50), "[Measures].[MonthCaption]") AS Month,
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CONVERT(NVARCHAR(50), "[Measures].[WeekNumberCaption]") AS Week_Number_In_Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
        CONVERT(NVARCHAR(50), "[Measures].[ProvinceCaption]") AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''PROVINCE'' AS LEVEL
		INTO STAGING_RAN5G_DAY_PROVINCE
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[DateCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[DateNumberCaption] AS [Date Time].[Date Number In Month].CURRENTMEMBER.NAME
            MEMBER [Measures].[MonthCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.PARENT.NAME
            MEMBER [Measures].[WeekNumberCaption] AS [Date Time].[Week Number In Year].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
            MEMBER [Measures].[ProvinceCaption] AS [DimRan5G].[Province].CURRENTMEMBER.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[DateCaption],
                [Measures].[DateNumberCaption],
                [Measures].[MonthCaption],
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[WeekNumberCaption],
                [Measures].[ProvinceCaption],
                [Measures].[CTKDCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Date].&['+@ngay+'] : [Date Time].[Calendar].[Date].&['+@ngay+'] *
                [Date Time].[Date Number In Month].[Date Number In Month].ALLMEMBERS *
                [Date Time].[Week Number In Year].[Week Number In Year].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML] *
                [DimRan5G].[Province].[Province].ALLMEMBERS *
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
');


--------------------CHI NHANH -----------------------
EXEC('
    SELECT 
        CONVERT(int, SUBSTRING("[Measures].[DateCaption]", 7, 10) + SUBSTRING("[Measures].[DateCaption]", 4, 2) + SUBSTRING("[Measures].[DateCaption]", 1, 2) + ''00'') AS DATETIME_ID,
        CONVERT(NVARCHAR(50), "[Measures].[DateNumberCaption]") AS Date_Number_In_Month,
        CONVERT(NVARCHAR(50), "[Measures].[MonthCaption]") AS Month,
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CONVERT(NVARCHAR(50), "[Measures].[WeekNumberCaption]") AS Week_Number_In_Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
        CONVERT(NVARCHAR(50), "[Measures].[BRANCHCaption]") AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''BRANCH'' AS LEVEL
		INTO STAGING_RAN5G_DAY_BRANCH
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[DateCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[DateNumberCaption] AS [Date Time].[Date Number In Month].CURRENTMEMBER.NAME
            MEMBER [Measures].[MonthCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.PARENT.NAME
            MEMBER [Measures].[WeekNumberCaption] AS [Date Time].[Week Number In Year].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
            MEMBER [Measures].[BRANCHCaption] AS [DimRan5G].[Chi Nhanh].CURRENTMEMBER.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[DateCaption],
                [Measures].[DateNumberCaption],
                [Measures].[MonthCaption],
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[WeekNumberCaption],
                [Measures].[BRANCHCaption],
                [Measures].[CTKDCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Date].&['+@ngay+'] : [Date Time].[Calendar].[Date].&['+@ngay+'] *
                [Date Time].[Date Number In Month].[Date Number In Month].ALLMEMBERS *
                [Date Time].[Week Number In Year].[Week Number In Year].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML] *
                [DimRan5G].[Chi Nhanh].[Chi Nhanh].ALLMEMBERS *
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
');




---------------TVT-----------------
EXEC('
    SELECT 
        CONVERT(int, SUBSTRING("[Measures].[DateCaption]", 7, 10) + SUBSTRING("[Measures].[DateCaption]", 4, 2) + SUBSTRING("[Measures].[DateCaption]", 1, 2) + ''00'') AS DATETIME_ID,
        CONVERT(NVARCHAR(50), "[Measures].[DateNumberCaption]") AS Date_Number_In_Month,
        CONVERT(NVARCHAR(50), "[Measures].[MonthCaption]") AS Month,
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CONVERT(NVARCHAR(50), "[Measures].[WeekNumberCaption]") AS Week_Number_In_Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTLD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
        CONVERT(NVARCHAR(50), "[Measures].[TVTCaption]") AS TVT,
        CONVERT(NVARCHAR(50), "[Measures].[DVTCaption]") AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''TVT'' AS LEVEL
		INTO STAGING_RAN5G_DAY_TVT
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[DateCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[DateNumberCaption] AS [Date Time].[Date Number In Month].CURRENTMEMBER.NAME
            MEMBER [Measures].[MonthCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.PARENT.NAME
            MEMBER [Measures].[WeekNumberCaption] AS [Date Time].[Week Number In Year].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
            MEMBER [Measures].[DVTCaption] AS [DimRan5G].[DVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[TVTCaption] AS [DimRan5G].[TVT].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[DateCaption],
                [Measures].[DateNumberCaption],
                [Measures].[MonthCaption],
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[WeekNumberCaption],
                [Measures].[TVTCaption],
                [Measures].[DVTCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Date].&['+@ngay+'] : [Date Time].[Calendar].[Date].&['+@ngay+'] *
                [Date Time].[Date Number In Month].[Date Number In Month].ALLMEMBERS *
                [Date Time].[Week Number In Year].[Week Number In Year].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML] *
                [DimRan5G].[DVT].[DVT].ALLMEMBERS *
                [DimRan5G].[TVT].[TVT].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
');



--------------- DVT ----------------------------
EXEC('
    SELECT 
        CONVERT(int, SUBSTRING("[Measures].[DateCaption]", 7, 10) + SUBSTRING("[Measures].[DateCaption]", 4, 2) + SUBSTRING("[Measures].[DateCaption]", 1, 2) + ''00'') AS DATETIME_ID,
        CONVERT(NVARCHAR(50), "[Measures].[DateNumberCaption]") AS Date_Number_In_Month,
        CONVERT(NVARCHAR(50), "[Measures].[MonthCaption]") AS Month,
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CONVERT(NVARCHAR(50), "[Measures].[WeekNumberCaption]") AS Week_Number_In_Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
        CONVERT(NVARCHAR(50), "[Measures].[DVTCaption]") AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''DVT'' AS LEVEL
		INTO STAGING_RAN5G_DAY_DVT
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[DateCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[DateNumberCaption] AS [Date Time].[Date Number In Month].CURRENTMEMBER.NAME
            MEMBER [Measures].[MonthCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.PARENT.NAME
            MEMBER [Measures].[WeekNumberCaption] AS [Date Time].[Week Number In Year].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
            MEMBER [Measures].[DVTCaption] AS [DimRan5G].[DVT].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[DateCaption],
                [Measures].[DateNumberCaption],
                [Measures].[MonthCaption],
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[WeekNumberCaption],
                [Measures].[DVTCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Date].&['+@ngay+'] : [Date Time].[Calendar].[Date].&['+@ngay+'] *
                [Date Time].[Date Number In Month].[Date Number In Month].ALLMEMBERS *
                [Date Time].[Week Number In Year].[Week Number In Year].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML] *
                [DimRan5G].[DVT].[DVT].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
');




-----------------CTKD ------------------
EXEC('
    SELECT 
        CONVERT(int, SUBSTRING("[Measures].[DateCaption]", 7, 10) + SUBSTRING("[Measures].[DateCaption]", 4, 2) + SUBSTRING("[Measures].[DateCaption]", 1, 2) + ''00'') AS DATETIME_ID,
        CONVERT(NVARCHAR(50), "[Measures].[DateNumberCaption]") AS Date_Number_In_Month,
        CONVERT(NVARCHAR(50), "[Measures].[MonthCaption]") AS Month,
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CONVERT(NVARCHAR(50), "[Measures].[WeekNumberCaption]") AS Week_Number_In_Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''CTKD'' AS LEVEL
		INTO STAGING_RAN5G_DAY_CTKD
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[DateCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[DateNumberCaption] AS [Date Time].[Date Number In Month].CURRENTMEMBER.NAME
            MEMBER [Measures].[MonthCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.PARENT.NAME
            MEMBER [Measures].[WeekNumberCaption] AS [Date Time].[Week Number In Year].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[DateCaption],
                [Measures].[DateNumberCaption],
                [Measures].[MonthCaption],
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[WeekNumberCaption],
                [Measures].[CTKDCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Date].&['+@ngay+'] : [Date Time].[Calendar].[Date].&['+@ngay+'] *
                [Date Time].[Date Number In Month].[Date Number In Month].ALLMEMBERS *
                [Date Time].[Week Number In Year].[Week Number In Year].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML] *
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
');



--------------------TTML-----------------------
EXEC('
    SELECT 
        CONVERT(int, SUBSTRING("[Measures].[DateCaption]", 7, 10) + SUBSTRING("[Measures].[DateCaption]", 4, 2) + SUBSTRING("[Measures].[DateCaption]", 1, 2) + ''00'') AS DATETIME_ID,
        CONVERT(NVARCHAR(50), "[Measures].[DateNumberCaption]") AS Date_Number_In_Month,
        CONVERT(NVARCHAR(50), "[Measures].[MonthCaption]") AS Month,
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CONVERT(NVARCHAR(50), "[Measures].[WeekNumberCaption]") AS Week_Number_In_Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''TTML'' AS LEVEL
		INTO STAGING_RAN5G_DAY_TTML
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[DateCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[DateNumberCaption] AS [Date Time].[Date Number In Month].CURRENTMEMBER.NAME
            MEMBER [Measures].[MonthCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.PARENT.NAME
            MEMBER [Measures].[WeekNumberCaption] AS [Date Time].[Week Number In Year].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[DateCaption],
                [Measures].[DateNumberCaption],
                [Measures].[MonthCaption],
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[WeekNumberCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Date].&['+@ngay+'] : [Date Time].[Calendar].[Date].&['+@ngay+'] *
                [Date Time].[Date Number In Month].[Date Number In Month].ALLMEMBERS *
                [Date Time].[Week Number In Year].[Week Number In Year].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML]
            } ON ROWS
        FROM ran5g'')
');

--------------VENDOR --------------------
EXEC('
    SELECT 
        CONVERT(int, SUBSTRING("[Measures].[DateCaption]", 7, 10) + SUBSTRING("[Measures].[DateCaption]", 4, 2) + SUBSTRING("[Measures].[DateCaption]", 1, 2) + ''00'') AS DATETIME_ID,
        CONVERT(NVARCHAR(50), "[Measures].[DateNumberCaption]") AS Date_Number_In_Month,
        CONVERT(NVARCHAR(50), "[Measures].[MonthCaption]") AS Month,
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CONVERT(NVARCHAR(50), "[Measures].[WeekNumberCaption]") AS Week_Number_In_Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTKD,
        CONVERT(NVARCHAR(50), "[Measures].[VendorCaption]") AS Vendor,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
		CAST(NULL AS NVARCHAR(50)) AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''VENDOR'' AS LEVEL
		INTO STAGING_RAN5G_DAY_VENDOR
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[DateCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[DateNumberCaption] AS [Date Time].[Date Number In Month].CURRENTMEMBER.NAME
            MEMBER [Measures].[MonthCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.PARENT.NAME
            MEMBER [Measures].[WeekNumberCaption] AS [Date Time].[Week Number In Year].CURRENTMEMBER.NAME
            MEMBER [Measures].[VendorCaption] AS [DimRan5G].[Vendor].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[DateCaption],
                [Measures].[DateNumberCaption],
                [Measures].[MonthCaption],
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[WeekNumberCaption],
                [Measures].[VendorCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Date].&['+@ngay+'] : [Date Time].[Calendar].[Date].&['+@ngay+'] *
                [Date Time].[Date Number In Month].[Date Number In Month].ALLMEMBERS *
                [Date Time].[Week Number In Year].[Week Number In Year].ALLMEMBERS *
                [DimRan5G].[Vendor].[Vendor].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
');



--------------------TOANMANG-----------------------
EXEC('
    SELECT 
        CONVERT(int, SUBSTRING("[Measures].[DateCaption]", 7, 10) + SUBSTRING("[Measures].[DateCaption]", 4, 2) + SUBSTRING("[Measures].[DateCaption]", 1, 2) + ''00'') AS DATETIME_ID,
        CONVERT(NVARCHAR(50), "[Measures].[DateNumberCaption]") AS Date_Number_In_Month,
        CONVERT(NVARCHAR(50), "[Measures].[MonthCaption]") AS Month,
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CONVERT(NVARCHAR(50), "[Measures].[WeekNumberCaption]") AS Week_Number_In_Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CAST(NULL AS NVARCHAR(50)) AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''TOANMANG'' AS LEVEL
		INTO STAGING_RAN5G_DAY_TOANMANG
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[DateCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[DateNumberCaption] AS [Date Time].[Date Number In Month].CURRENTMEMBER.NAME
            MEMBER [Measures].[MonthCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.PARENT.NAME
            MEMBER [Measures].[WeekNumberCaption] AS [Date Time].[Week Number In Year].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[DateCaption],
                [Measures].[DateNumberCaption],
                [Measures].[MonthCaption],
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[WeekNumberCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Date].&['+@ngay+'] : [Date Time].[Calendar].[Date].&['+@ngay+'] *
                [Date Time].[Date Number In Month].[Date Number In Month].ALLMEMBERS *
                [Date Time].[Week Number In Year].[Week Number In Year].ALLMEMBERS 
            } ON ROWS
        FROM ran5g'')
');



		DELETE FROM MART_RAN5G_REPORT_DAILY WHERE DATETIME_ID = CAST(@Ngay + '00' AS INT);

		INSERT INTO MART_RAN5G_REPORT_DAILY 
		SELECT * FROM(
		SELECT * FROM  STAGING_RAN5G_DAY_DISTRICT WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_DAY_PROVINCE WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_DAY_BRANCH WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_DAY_CTKD WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_DAY_TTML WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_DAY_VENDOR WHERE VENDOR <> 'SAMSUNG' AND DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_DAY_DVT WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_DAY_TVT WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_DAY_TOANMANG WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0) TEMP


		DROP TABLE IF EXISTS STAGING_RAN5G_DAY_DISTRICT;
		DROP TABLE IF EXISTS STAGING_RAN5G_DAY_PROVINCE;
		DROP TABLE IF EXISTS STAGING_RAN5G_DAY_BRANCH;
		DROP TABLE IF EXISTS STAGING_RAN5G_DAY_CTKD;
		DROP TABLE IF EXISTS STAGING_RAN5G_DAY_TTML;
		DROP TABLE IF EXISTS STAGING_RAN5G_DAY_VENDOR;
		DROP TABLE IF EXISTS STAGING_RAN5G_DAY_DVT;
		DROP TABLE IF EXISTS STAGING_RAN5G_DAY_TVT;
		DROP TABLE IF EXISTS STAGING_RAN5G_DAY_TOANMANG;


END

GO

/****** Object:  StoredProcedure [dbo].[RAN_5G_DAY_QUERY]    Script Date: 5/19/2025 10:23:17 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [dbo].[RAN_5G_DAY_QUERY] 
	@Ngay varchar(100) = '',
	@last int = 1
AS
BEGIN
	SET NOCOUNT ON;

	SET @ngay = COALESCE(NULLIF(@ngay, ''), CONVERT(VARCHAR(10), GETDATE()-@last, 112));
	
	DECLARE @ngay_103 varchar(10) = CONVERT(VARCHAR(10), convert(datetime, @ngay),103)

	declare @datetime_id int = convert(int, @ngay) * 100
	

	EXEC RAN_5G_DAY_GENERAL @Ngay = @Ngay
	EXEC RAN_5G_WEEK_GENERAL @Ngay = @Ngay
	EXEC RAN_5G_MONTH_GENERAL @ngay = @Ngay
	EXEC RAN_5G_QUARTER_GENERAL @ngay = @Ngay
	EXEC RAN_5G_YEAR_GENERAL @ngay = @Ngay
END
GO

/****** Object:  StoredProcedure [dbo].[RAN_5G_MONTH_GENERAL]    Script Date: 5/19/2025 10:23:17 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[RAN_5G_MONTH_GENERAL] 
	@Ngay varchar(100) = '',
	@last int = 1
AS
BEGIN
	SET NOCOUNT ON;

	SET @ngay = COALESCE(NULLIF(@ngay, ''), CONVERT(VARCHAR(10), GETDATE()-@last, 112));
	
	DECLARE @ngay_103 varchar(10) = CONVERT(VARCHAR(10), convert(datetime, @ngay),103)

	DECLARE @datetime_id int = convert(int, @ngay) * 100

	DECLARE @month_id int = (SELECT TOP (1) MONTH_ID FROM DIM_DATE_TIME WHERE DATE_ID = @ngay)


	DROP TABLE IF EXISTS STAGING_RAN5G_MONTH_DISTRICT;
	DROP TABLE IF EXISTS STAGING_RAN5G_MONTH_PROVINCE;
	DROP TABLE IF EXISTS STAGING_RAN5G_MONTH_BRANCH;
	DROP TABLE IF EXISTS STAGING_RAN5G_MONTH_CTKD;
	DROP TABLE IF EXISTS STAGING_RAN5G_MONTH_TTML;
	DROP TABLE IF EXISTS STAGING_RAN5G_MONTH_VENDOR;
	DROP TABLE IF EXISTS STAGING_RAN5G_MONTH_DVT;
	DROP TABLE IF EXISTS STAGING_RAN5G_MONTH_TVT;
	DROP TABLE IF EXISTS STAGING_RAN5G_MONTH_TOANMANG;


----------------DISTRICT--------------------

EXEC('
    SELECT 
		'+@month_id+' AS MONTH_ID,
        CONVERT(NVARCHAR(50), "[Measures].[MonthCaption]") AS Month,
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CONVERT(NVARCHAR(50), "[Measures].[DistrictCaption]") AS District,
        CONVERT(NVARCHAR(50), "[Measures].[ProvinceCaption]") AS Province,
        CONVERT(NVARCHAR(50), "[Measures].[ChiNhanhCaption]") AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
        CONVERT(NVARCHAR(50), "[Measures].[TVTCaption]") AS TVT,
        CONVERT(NVARCHAR(50), "[Measures].[DVTCaption]") AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''DISTRICT'' AS LEVEL
		INTO STAGING_RAN5G_MONTH_DISTRICT
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[MonthCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.NAME
            MEMBER [Measures].[DistrictCaption] AS [DimRan5G].[DISTRICT].CURRENTMEMBER.NAME
            MEMBER [Measures].[ProvinceCaption] AS [DimRan5G].[PROVINCE].CURRENTMEMBER.NAME
            MEMBER [Measures].[ChiNhanhCaption] AS [DimRan5G].[Chi Nhanh].CURRENTMEMBER.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
            MEMBER [Measures].[TVTCaption] AS [DimRan5G].[TVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[DVTCaption] AS [DimRan5G].[DVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[MonthCaption],
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[DistrictCaption],
                [Measures].[ProvinceCaption],
                [Measures].[ChiNhanhCaption],
                [Measures].[CTKDCaption],
                [Measures].[TVTCaption],
                [Measures].[DVTCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Month].&['+@month_id+'] *
                [DimRan5G].[DISTRICT].[DISTRICT].ALLMEMBERS *
                [DimRan5G].[PROVINCE].[PROVINCE].ALLMEMBERS *
                [DimRan5G].[Chi Nhanh].[Chi Nhanh].ALLMEMBERS *
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS *
                [DimRan5G].[TVT].[TVT].ALLMEMBERS *
                [DimRan5G].[DVT].[DVT].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')

------------------PROVINCE----------------------
EXEC('
    SELECT 
		'+@month_id+' AS MONTH_ID,
        CONVERT(NVARCHAR(50), "[Measures].[MonthCaption]") AS Month,
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
        CONVERT(NVARCHAR(50), "[Measures].[ProvinceCaption]") AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''PROVINCE'' AS LEVEL
		INTO STAGING_RAN5G_MONTH_PROVINCE
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[MonthCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.NAME
            MEMBER [Measures].[ProvinceCaption] AS [DimRan5G].[PROVINCE].CURRENTMEMBER.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[MonthCaption],
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[ProvinceCaption],
                [Measures].[CTKDCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Month].&['+@month_id+'] *
                [DimRan5G].[PROVINCE].[PROVINCE].ALLMEMBERS *
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')

--------------CHI NHANH --------------------
EXEC('
    SELECT 
		'+@month_id+' AS MONTH_ID,
        CONVERT(NVARCHAR(50), "[Measures].[MonthCaption]") AS Month,
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
        CONVERT(NVARCHAR(50), "[Measures].[ChiNhanhCaption]") AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''BRANCH'' AS LEVEL
		INTO STAGING_RAN5G_MONTH_BRANCH
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[MonthCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.NAME
            MEMBER [Measures].[ChiNhanhCaption] AS [DimRan5G].[Chi Nhanh].CURRENTMEMBER.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[MonthCaption],
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[ChiNhanhCaption],
                [Measures].[CTKDCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Month].&['+@month_id+'] *
                [DimRan5G].[Chi Nhanh].[Chi Nhanh].ALLMEMBERS *
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')


-------------------TVT--------------------
EXEC('
    SELECT 
		'+@month_id+' AS MONTH_ID,
        CONVERT(NVARCHAR(50), "[Measures].[MonthCaption]") AS Month,
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTLD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
        CONVERT(NVARCHAR(50), "[Measures].[TVTCaption]") AS TVT,
        CONVERT(NVARCHAR(50), "[Measures].[DVTCaption]") AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''TVT'' AS LEVEL
		INTO STAGING_RAN5G_MONTH_TVT
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[MonthCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.NAME
            MEMBER [Measures].[TVTCaption] AS [DimRan5G].[TVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[DVTCaption] AS [DimRan5G].[DVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[MonthCaption],
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[TVTCaption],
                [Measures].[DVTCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Month].&['+@month_id+'] *
                [DimRan5G].[TVT].[TVT].ALLMEMBERS *
                [DimRan5G].[DVT].[DVT].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')

---------------------DVT-----------------------
EXEC('
    SELECT 
		'+@month_id+' AS MONTH_ID,
        CONVERT(NVARCHAR(50), "[Measures].[MonthCaption]") AS Month,
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
        CONVERT(NVARCHAR(50), "[Measures].[DVTCaption]") AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''DVT'' AS LEVEL
		INTO STAGING_RAN5G_MONTH_DVT
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[MonthCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.NAME
            MEMBER [Measures].[DVTCaption] AS [DimRan5G].[DVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[MonthCaption],
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[DVTCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Month].&['+@month_id+'] *
                [DimRan5G].[DVT].[DVT].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')


-------------------CTKD----------------------
EXEC('
    SELECT 
		'+@month_id+' AS MONTH_ID,
        CONVERT(NVARCHAR(50), "[Measures].[MonthCaption]") AS Month,
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''CTKD'' AS LEVEL
		INTO STAGING_RAN5G_MONTH_CTKD
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[MonthCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[MonthCaption],
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[CTKDCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Month].&['+@month_id+'] *
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')

-------------------TTML-------------------

EXEC('
    SELECT 
		'+@month_id+' AS MONTH_ID,
        CONVERT(NVARCHAR(50), "[Measures].[MonthCaption]") AS Month,
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''TTML'' AS LEVEL
		INTO STAGING_RAN5G_MONTH_TTML
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[MonthCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[MonthCaption],
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Month].&['+@month_id+'] *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')


----------------VENDOR-------------------											

EXEC('
    SELECT 
		'+@month_id+' AS MONTH_ID,
        CONVERT(NVARCHAR(50), "[Measures].[MonthCaption]") AS Month,
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTKD,
        CONVERT(NVARCHAR(50), "[Measures].[VendorCaption]") AS Vendor,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
		CAST(NULL AS NVARCHAR(50)) AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''VENDOR'' AS LEVEL
		INTO STAGING_RAN5G_MONTH_VENDOR
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[MonthCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.NAME
            MEMBER [Measures].[VendorCaption] AS [DimRan5G].[Vendor].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[MonthCaption],
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[VendorCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Month].&['+@month_id+'] *
                [DimRan5G].[Vendor].[Vendor].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')



-------------------TOANMANG-------------------

EXEC('
    SELECT 
		'+@month_id+' AS MONTH_ID,
        CONVERT(NVARCHAR(50), "[Measures].[MonthCaption]") AS Month,
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CAST(NULL AS NVARCHAR(50)) AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''TOANMANG'' AS LEVEL
		INTO STAGING_RAN5G_MONTH_TOANMANG
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[MonthCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.NAME
        SELECT
            {
                [Measures].[MonthCaption],
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Month].&['+@month_id+'] 
            } ON ROWS
        FROM ran5g'')
')


		
		DELETE FROM MART_RAN5G_REPORT_MONTHLY WHERE MONTH_ID = @month_id;

		INSERT INTO MART_RAN5G_REPORT_MONTHLY
		SELECT * FROM(
		SELECT * FROM  STAGING_RAN5G_MONTH_DISTRICT WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_MONTH_PROVINCE WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_MONTH_BRANCH WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_MONTH_CTKD WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_MONTH_TTML WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_MONTH_VENDOR WHERE VENDOR <> 'SAMSUNG' AND DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_MONTH_DVT WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_MONTH_TVT WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_MONTH_TOANMANG WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0) TEMP


		DROP TABLE IF EXISTS STAGING_RAN5G_MONTH_DISTRICT;
		DROP TABLE IF EXISTS STAGING_RAN5G_MONTH_PROVINCE;
		DROP TABLE IF EXISTS STAGING_RAN5G_MONTH_BRANCH;
		DROP TABLE IF EXISTS STAGING_RAN5G_MONTH_CTKD;
		DROP TABLE IF EXISTS STAGING_RAN5G_MONTH_TTML;
		DROP TABLE IF EXISTS STAGING_RAN5G_MONTH_VENDOR;
		DROP TABLE IF EXISTS STAGING_RAN5G_MONTH_DVT;
		DROP TABLE IF EXISTS STAGING_RAN5G_MONTH_TVT;
		DROP TABLE IF EXISTS STAGING_RAN5G_MONTH_TOANMANG;
END

GO

/****** Object:  StoredProcedure [dbo].[RAN_5G_QUARTER_GENERAL]    Script Date: 5/19/2025 10:23:17 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[RAN_5G_QUARTER_GENERAL]
	@Ngay varchar(100) = '',
	@last int = 1
AS
BEGIN
	SET NOCOUNT ON;

	SET @ngay = COALESCE(NULLIF(@ngay, ''), CONVERT(VARCHAR(10), GETDATE()-@last, 112));
	
	DECLARE @ngay_103 varchar(10) = CONVERT(VARCHAR(10), convert(datetime, @ngay),103)

	DECLARE @datetime_id int = convert(int, @ngay) * 100

	DECLARE @quarter_id varchar(10) = (SELECT TOP (1) QUARTER_ID FROM DIM_DATE_TIME WHERE DATE_ID = @ngay)


	DROP TABLE IF EXISTS STAGING_RAN5G_QUARTER_DISTRICT;
	DROP TABLE IF EXISTS STAGING_RAN5G_QUARTER_PROVINCE;
	DROP TABLE IF EXISTS STAGING_RAN5G_QUARTER_BRANCH;
	DROP TABLE IF EXISTS STAGING_RAN5G_QUARTER_CTKD;
	DROP TABLE IF EXISTS STAGING_RAN5G_QUARTER_TTML;
	DROP TABLE IF EXISTS STAGING_RAN5G_QUARTER_VENDOR;
	DROP TABLE IF EXISTS STAGING_RAN5G_QUARTER_DVT;
	DROP TABLE IF EXISTS STAGING_RAN5G_QUARTER_TVT;
	DROP TABLE IF EXISTS STAGING_RAN5G_QUARTER_TOANMANG;

----------------DISTRICT--------------

EXEC('
    SELECT 
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CONVERT(NVARCHAR(50), "[Measures].[DistrictCaption]") AS District,
        CONVERT(NVARCHAR(50), "[Measures].[ProvinceCaption]") AS Province,
        CONVERT(NVARCHAR(50), "[Measures].[ChiNhanhCaption]") AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
        CONVERT(NVARCHAR(50), "[Measures].[TVTCaption]") AS TVT,
        CONVERT(NVARCHAR(50), "[Measures].[DVTCaption]") AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR]")) AS Cell_UL_THP_NR,
		''DISTRICT'' AS LEVEL
		INTO STAGING_RAN5G_QUARTER_DISTRICT
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[DistrictCaption] AS [DimRan5G].[DISTRICT].CURRENTMEMBER.NAME
            MEMBER [Measures].[ProvinceCaption] AS [DimRan5G].[PROVINCE].CURRENTMEMBER.NAME
            MEMBER [Measures].[ChiNhanhCaption] AS [DimRan5G].[Chi Nhanh].CURRENTMEMBER.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
            MEMBER [Measures].[TVTCaption] AS [DimRan5G].[TVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[DVTCaption] AS [DimRan5G].[DVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
            MEMBER [Measures].[PS_CSSR_NR] AS KPIValue("PS_CSSR_NR")
            MEMBER [Measures].[SDR_NR] AS KPIValue("SDR_NR")
            MEMBER [Measures].[PRB_Util_DL_NR] AS KPIValue("PRB_Util_DL_NR")
            MEMBER [Measures].[PRB_Util_UL_NR] AS KPIValue("PRB_Util_UL_NR")
            MEMBER [Measures].[Latency_NR] AS KPIValue("Latency_NR")
            MEMBER [Measures].[PktLossR] AS KPIValue("PktLossR")
            MEMBER [Measures].[Connected_RRC_user_average] AS KPIValue("Connected_RRC_user_average")
            MEMBER [Measures].[Connected_RRC_user_Max] AS KPIValue("Connected_RRC_user_Max")
            MEMBER [Measures].[DKD5G_NR] AS KPIValue("DKD5G_NR")
            MEMBER [Measures].[EN_DC_SR_NR] AS KPIValue("EN_DC_SR_NR")
            MEMBER [Measures].[HOSR_NR] AS KPIValue("HOSR_NR")
            MEMBER [Measures].[RASR_NR] AS KPIValue("RASR_NR")
            MEMBER [Measures].[DL_TRAFFIC_NR] AS KPIValue("DL_TRAFFIC_NR")
            MEMBER [Measures].[UL_TRAFFIC_NR] AS KPIValue("UL_TRAFFIC_NR")
            MEMBER [Measures].[User_DL_THP_NR] AS KPIValue("User_DL_THP_NR")
            MEMBER [Measures].[User_UL_THP_NR] AS KPIValue("User_UL_THP_NR")
            MEMBER [Measures].[Cell_DL_THP_NR] AS KPIValue("Cell_DL_THP_NR")
            MEMBER [Measures].[Cell_UL_THP_NR] AS KPIValue("Cell_UL_THP_NR")
        SELECT
            {
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[DistrictCaption],
                [Measures].[ProvinceCaption],
                [Measures].[ChiNhanhCaption],
                [Measures].[CTKDCaption],
                [Measures].[TVTCaption],
                [Measures].[DVTCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR],
                [Measures].[SDR_NR],
                [Measures].[PRB_Util_DL_NR],
                [Measures].[PRB_Util_UL_NR],
                [Measures].[Latency_NR],
                [Measures].[PktLossR],
                [Measures].[Connected_RRC_user_average],
                [Measures].[Connected_RRC_user_Max],
                [Measures].[DKD5G_NR],
                [Measures].[EN_DC_SR_NR],
                [Measures].[HOSR_NR],
                [Measures].[RASR_NR],
                [Measures].[DL_TRAFFIC_NR],
                [Measures].[UL_TRAFFIC_NR],
                [Measures].[User_DL_THP_NR],
                [Measures].[User_UL_THP_NR],
                [Measures].[Cell_DL_THP_NR],
                [Measures].[Cell_UL_THP_NR]
            } ON COLUMNS,
            NON EMPTY {
                [DimRan5G].[DISTRICT].[DISTRICT].ALLMEMBERS *
                [DimRan5G].[PROVINCE].[PROVINCE].ALLMEMBERS *
                [DimRan5G].[Chi Nhanh].[Chi Nhanh].ALLMEMBERS *
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS *
                [DimRan5G].[TVT].[TVT].ALLMEMBERS *
                [DimRan5G].[DVT].[DVT].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS *
                [Date Time].[Calendar].[Quarter].ALLMEMBERS
            } DIMENSION PROPERTIES MEMBER_CAPTION, MEMBER_UNIQUE_NAME ON ROWS
        FROM (
            SELECT { [Date Time].[Calendar].[Quarter].&['+@quarter_id+'] } ON COLUMNS
            FROM [ran5g]
        )
        CELL PROPERTIES VALUE, BACK_COLOR, FORE_COLOR, FORMATTED_VALUE, FORMAT_STRING, FONT_NAME, FONT_SIZE, FONT_FLAGS
    '')
')



----------------PROVINCE--------------

EXEC('
    SELECT 
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CAST(NULL AS NVARCHAR(50)) AS District,
        CONVERT(NVARCHAR(50), "[Measures].[ProvinceCaption]") AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR]")) AS Cell_UL_THP_NR,
		''PROVINCE'' AS LEVEL
		INTO STAGING_RAN5G_QUARTER_PROVINCE
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[ProvinceCaption] AS [DimRan5G].[PROVINCE].CURRENTMEMBER.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
            MEMBER [Measures].[PS_CSSR_NR] AS KPIValue("PS_CSSR_NR")
            MEMBER [Measures].[SDR_NR] AS KPIValue("SDR_NR")
            MEMBER [Measures].[PRB_Util_DL_NR] AS KPIValue("PRB_Util_DL_NR")
            MEMBER [Measures].[PRB_Util_UL_NR] AS KPIValue("PRB_Util_UL_NR")
            MEMBER [Measures].[Latency_NR] AS KPIValue("Latency_NR")
            MEMBER [Measures].[PktLossR] AS KPIValue("PktLossR")
            MEMBER [Measures].[Connected_RRC_user_average] AS KPIValue("Connected_RRC_user_average")
            MEMBER [Measures].[Connected_RRC_user_Max] AS KPIValue("Connected_RRC_user_Max")
            MEMBER [Measures].[DKD5G_NR] AS KPIValue("DKD5G_NR")
            MEMBER [Measures].[EN_DC_SR_NR] AS KPIValue("EN_DC_SR_NR")
            MEMBER [Measures].[HOSR_NR] AS KPIValue("HOSR_NR")
            MEMBER [Measures].[RASR_NR] AS KPIValue("RASR_NR")
            MEMBER [Measures].[DL_TRAFFIC_NR] AS KPIValue("DL_TRAFFIC_NR")
            MEMBER [Measures].[UL_TRAFFIC_NR] AS KPIValue("UL_TRAFFIC_NR")
            MEMBER [Measures].[User_DL_THP_NR] AS KPIValue("User_DL_THP_NR")
            MEMBER [Measures].[User_UL_THP_NR] AS KPIValue("User_UL_THP_NR")
            MEMBER [Measures].[Cell_DL_THP_NR] AS KPIValue("Cell_DL_THP_NR")
            MEMBER [Measures].[Cell_UL_THP_NR] AS KPIValue("Cell_UL_THP_NR")
        SELECT
            {
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[ProvinceCaption],
                [Measures].[CTKDCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR],
                [Measures].[SDR_NR],
                [Measures].[PRB_Util_DL_NR],
                [Measures].[PRB_Util_UL_NR],
                [Measures].[Latency_NR],
                [Measures].[PktLossR],
                [Measures].[Connected_RRC_user_average],
                [Measures].[Connected_RRC_user_Max],
                [Measures].[DKD5G_NR],
                [Measures].[EN_DC_SR_NR],
                [Measures].[HOSR_NR],
                [Measures].[RASR_NR],
                [Measures].[DL_TRAFFIC_NR],
                [Measures].[UL_TRAFFIC_NR],
                [Measures].[User_DL_THP_NR],
                [Measures].[User_UL_THP_NR],
                [Measures].[Cell_DL_THP_NR],
                [Measures].[Cell_UL_THP_NR]
            } ON COLUMNS,
            NON EMPTY {
                [DimRan5G].[PROVINCE].[PROVINCE].ALLMEMBERS *
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS *
                [Date Time].[Calendar].[Quarter].ALLMEMBERS
            } DIMENSION PROPERTIES MEMBER_CAPTION, MEMBER_UNIQUE_NAME ON ROWS
        FROM (
            SELECT { [Date Time].[Calendar].[Quarter].&['+@quarter_id+'] } ON COLUMNS
            FROM [ran5g]
        )
        CELL PROPERTIES VALUE, BACK_COLOR, FORE_COLOR, FORMATTED_VALUE, FORMAT_STRING, FONT_NAME, FONT_SIZE, FONT_FLAGS
    '')
')


----------------CHI_NHANH--------------

EXEC('
    SELECT 
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
        CONVERT(NVARCHAR(50), "[Measures].[ChiNhanhCaption]") AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR]")) AS Cell_UL_THP_NR,
		''BRANCH'' AS LEVEL
		INTO STAGING_RAN5G_QUARTER_BRANCH
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[ChiNhanhCaption] AS [DimRan5G].[Chi Nhanh].CURRENTMEMBER.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
            MEMBER [Measures].[PS_CSSR_NR] AS KPIValue("PS_CSSR_NR")
            MEMBER [Measures].[SDR_NR] AS KPIValue("SDR_NR")
            MEMBER [Measures].[PRB_Util_DL_NR] AS KPIValue("PRB_Util_DL_NR")
            MEMBER [Measures].[PRB_Util_UL_NR] AS KPIValue("PRB_Util_UL_NR")
            MEMBER [Measures].[Latency_NR] AS KPIValue("Latency_NR")
            MEMBER [Measures].[PktLossR] AS KPIValue("PktLossR")
            MEMBER [Measures].[Connected_RRC_user_average] AS KPIValue("Connected_RRC_user_average")
            MEMBER [Measures].[Connected_RRC_user_Max] AS KPIValue("Connected_RRC_user_Max")
            MEMBER [Measures].[DKD5G_NR] AS KPIValue("DKD5G_NR")
            MEMBER [Measures].[EN_DC_SR_NR] AS KPIValue("EN_DC_SR_NR")
            MEMBER [Measures].[HOSR_NR] AS KPIValue("HOSR_NR")
            MEMBER [Measures].[RASR_NR] AS KPIValue("RASR_NR")
            MEMBER [Measures].[DL_TRAFFIC_NR] AS KPIValue("DL_TRAFFIC_NR")
            MEMBER [Measures].[UL_TRAFFIC_NR] AS KPIValue("UL_TRAFFIC_NR")
            MEMBER [Measures].[User_DL_THP_NR] AS KPIValue("User_DL_THP_NR")
            MEMBER [Measures].[User_UL_THP_NR] AS KPIValue("User_UL_THP_NR")
            MEMBER [Measures].[Cell_DL_THP_NR] AS KPIValue("Cell_DL_THP_NR")
            MEMBER [Measures].[Cell_UL_THP_NR] AS KPIValue("Cell_UL_THP_NR")
        SELECT
            {
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[ChiNhanhCaption],
                [Measures].[CTKDCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR],
                [Measures].[SDR_NR],
                [Measures].[PRB_Util_DL_NR],
                [Measures].[PRB_Util_UL_NR],
                [Measures].[Latency_NR],
                [Measures].[PktLossR],
                [Measures].[Connected_RRC_user_average],
                [Measures].[Connected_RRC_user_Max],
                [Measures].[DKD5G_NR],
                [Measures].[EN_DC_SR_NR],
                [Measures].[HOSR_NR],
                [Measures].[RASR_NR],
                [Measures].[DL_TRAFFIC_NR],
                [Measures].[UL_TRAFFIC_NR],
                [Measures].[User_DL_THP_NR],
                [Measures].[User_UL_THP_NR],
                [Measures].[Cell_DL_THP_NR],
                [Measures].[Cell_UL_THP_NR]
            } ON COLUMNS,
            NON EMPTY {
                [DimRan5G].[Chi Nhanh].[Chi Nhanh].ALLMEMBERS *
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS *
                [Date Time].[Calendar].[Quarter].ALLMEMBERS
            } DIMENSION PROPERTIES MEMBER_CAPTION, MEMBER_UNIQUE_NAME ON ROWS
        FROM (
            SELECT { [Date Time].[Calendar].[Quarter].&['+@quarter_id+'] } ON COLUMNS
            FROM [ran5g]
        )
        CELL PROPERTIES VALUE, BACK_COLOR, FORE_COLOR, FORMATTED_VALUE, FORMAT_STRING, FONT_NAME, FONT_SIZE, FONT_FLAGS
    '')
')


----------------TVT--------------

EXEC('
    SELECT 
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTLD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
        CONVERT(NVARCHAR(50), "[Measures].[TVTCaption]") AS TVT,
        CONVERT(NVARCHAR(50), "[Measures].[DVTCaption]") AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR]")) AS Cell_UL_THP_NR,
		''TVT'' AS LEVEL
		INTO STAGING_RAN5G_QUARTER_TVT
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[TVTCaption] AS [DimRan5G].[TVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[DVTCaption] AS [DimRan5G].[DVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
            MEMBER [Measures].[PS_CSSR_NR] AS KPIValue("PS_CSSR_NR")
            MEMBER [Measures].[SDR_NR] AS KPIValue("SDR_NR")
            MEMBER [Measures].[PRB_Util_DL_NR] AS KPIValue("PRB_Util_DL_NR")
            MEMBER [Measures].[PRB_Util_UL_NR] AS KPIValue("PRB_Util_UL_NR")
            MEMBER [Measures].[Latency_NR] AS KPIValue("Latency_NR")
            MEMBER [Measures].[PktLossR] AS KPIValue("PktLossR")
            MEMBER [Measures].[Connected_RRC_user_average] AS KPIValue("Connected_RRC_user_average")
            MEMBER [Measures].[Connected_RRC_user_Max] AS KPIValue("Connected_RRC_user_Max")
            MEMBER [Measures].[DKD5G_NR] AS KPIValue("DKD5G_NR")
            MEMBER [Measures].[EN_DC_SR_NR] AS KPIValue("EN_DC_SR_NR")
            MEMBER [Measures].[HOSR_NR] AS KPIValue("HOSR_NR")
            MEMBER [Measures].[RASR_NR] AS KPIValue("RASR_NR")
            MEMBER [Measures].[DL_TRAFFIC_NR] AS KPIValue("DL_TRAFFIC_NR")
            MEMBER [Measures].[UL_TRAFFIC_NR] AS KPIValue("UL_TRAFFIC_NR")
            MEMBER [Measures].[User_DL_THP_NR] AS KPIValue("User_DL_THP_NR")
            MEMBER [Measures].[User_UL_THP_NR] AS KPIValue("User_UL_THP_NR")
            MEMBER [Measures].[Cell_DL_THP_NR] AS KPIValue("Cell_DL_THP_NR")
            MEMBER [Measures].[Cell_UL_THP_NR] AS KPIValue("Cell_UL_THP_NR")
        SELECT
            {
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[TVTCaption],
                [Measures].[DVTCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR],
                [Measures].[SDR_NR],
                [Measures].[PRB_Util_DL_NR],
                [Measures].[PRB_Util_UL_NR],
                [Measures].[Latency_NR],
                [Measures].[PktLossR],
                [Measures].[Connected_RRC_user_average],
                [Measures].[Connected_RRC_user_Max],
                [Measures].[DKD5G_NR],
                [Measures].[EN_DC_SR_NR],
                [Measures].[HOSR_NR],
                [Measures].[RASR_NR],
                [Measures].[DL_TRAFFIC_NR],
                [Measures].[UL_TRAFFIC_NR],
                [Measures].[User_DL_THP_NR],
                [Measures].[User_UL_THP_NR],
                [Measures].[Cell_DL_THP_NR],
                [Measures].[Cell_UL_THP_NR]
            } ON COLUMNS,
            NON EMPTY {
                [DimRan5G].[TVT].[TVT].ALLMEMBERS *
                [DimRan5G].[DVT].[DVT].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS *
                [Date Time].[Calendar].[Quarter].ALLMEMBERS
            } DIMENSION PROPERTIES MEMBER_CAPTION, MEMBER_UNIQUE_NAME ON ROWS
        FROM (
            SELECT { [Date Time].[Calendar].[Quarter].&['+@quarter_id+'] } ON COLUMNS
            FROM [ran5g]
        )
        CELL PROPERTIES VALUE, BACK_COLOR, FORE_COLOR, FORMATTED_VALUE, FORMAT_STRING, FONT_NAME, FONT_SIZE, FONT_FLAGS
    '')
')


----------------DVT--------------

EXEC('
    SELECT 
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTLD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
        CAST(NULL AS NVARCHAR(50)) AS TVT,
        CONVERT(NVARCHAR(50), "[Measures].[DVTCaption]") AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR]")) AS Cell_UL_THP_NR,
		''DVT'' AS LEVEL
		INTO STAGING_RAN5G_QUARTER_DVT
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[DVTCaption] AS [DimRan5G].[DVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
            MEMBER [Measures].[PS_CSSR_NR] AS KPIValue("PS_CSSR_NR")
            MEMBER [Measures].[SDR_NR] AS KPIValue("SDR_NR")
            MEMBER [Measures].[PRB_Util_DL_NR] AS KPIValue("PRB_Util_DL_NR")
            MEMBER [Measures].[PRB_Util_UL_NR] AS KPIValue("PRB_Util_UL_NR")
            MEMBER [Measures].[Latency_NR] AS KPIValue("Latency_NR")
            MEMBER [Measures].[PktLossR] AS KPIValue("PktLossR")
            MEMBER [Measures].[Connected_RRC_user_average] AS KPIValue("Connected_RRC_user_average")
            MEMBER [Measures].[Connected_RRC_user_Max] AS KPIValue("Connected_RRC_user_Max")
            MEMBER [Measures].[DKD5G_NR] AS KPIValue("DKD5G_NR")
            MEMBER [Measures].[EN_DC_SR_NR] AS KPIValue("EN_DC_SR_NR")
            MEMBER [Measures].[HOSR_NR] AS KPIValue("HOSR_NR")
            MEMBER [Measures].[RASR_NR] AS KPIValue("RASR_NR")
            MEMBER [Measures].[DL_TRAFFIC_NR] AS KPIValue("DL_TRAFFIC_NR")
            MEMBER [Measures].[UL_TRAFFIC_NR] AS KPIValue("UL_TRAFFIC_NR")
            MEMBER [Measures].[User_DL_THP_NR] AS KPIValue("User_DL_THP_NR")
            MEMBER [Measures].[User_UL_THP_NR] AS KPIValue("User_UL_THP_NR")
            MEMBER [Measures].[Cell_DL_THP_NR] AS KPIValue("Cell_DL_THP_NR")
            MEMBER [Measures].[Cell_UL_THP_NR] AS KPIValue("Cell_UL_THP_NR")
        SELECT
            {
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[DVTCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR],
                [Measures].[SDR_NR],
                [Measures].[PRB_Util_DL_NR],
                [Measures].[PRB_Util_UL_NR],
                [Measures].[Latency_NR],
                [Measures].[PktLossR],
                [Measures].[Connected_RRC_user_average],
                [Measures].[Connected_RRC_user_Max],
                [Measures].[DKD5G_NR],
                [Measures].[EN_DC_SR_NR],
                [Measures].[HOSR_NR],
                [Measures].[RASR_NR],
                [Measures].[DL_TRAFFIC_NR],
                [Measures].[UL_TRAFFIC_NR],
                [Measures].[User_DL_THP_NR],
                [Measures].[User_UL_THP_NR],
                [Measures].[Cell_DL_THP_NR],
                [Measures].[Cell_UL_THP_NR]
            } ON COLUMNS,
            NON EMPTY {
                [DimRan5G].[DVT].[DVT].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS *
                [Date Time].[Calendar].[Quarter].ALLMEMBERS
            } DIMENSION PROPERTIES MEMBER_CAPTION, MEMBER_UNIQUE_NAME ON ROWS
        FROM (
            SELECT { [Date Time].[Calendar].[Quarter].&['+@quarter_id+'] } ON COLUMNS
            FROM [ran5g]
        )
        CELL PROPERTIES VALUE, BACK_COLOR, FORE_COLOR, FORMATTED_VALUE, FORMAT_STRING, FONT_NAME, FONT_SIZE, FONT_FLAGS
    '')
')


----------------TTML--------------

EXEC('
    SELECT 
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTLD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
        CAST(NULL AS NVARCHAR(50)) AS TVT,
        CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR]")) AS Cell_UL_THP_NR,
		''TTML'' AS LEVEL
		INTO STAGING_RAN5G_QUARTER_TTML
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
            MEMBER [Measures].[PS_CSSR_NR] AS KPIValue("PS_CSSR_NR")
            MEMBER [Measures].[SDR_NR] AS KPIValue("SDR_NR")
            MEMBER [Measures].[PRB_Util_DL_NR] AS KPIValue("PRB_Util_DL_NR")
            MEMBER [Measures].[PRB_Util_UL_NR] AS KPIValue("PRB_Util_UL_NR")
            MEMBER [Measures].[Latency_NR] AS KPIValue("Latency_NR")
            MEMBER [Measures].[PktLossR] AS KPIValue("PktLossR")
            MEMBER [Measures].[Connected_RRC_user_average] AS KPIValue("Connected_RRC_user_average")
            MEMBER [Measures].[Connected_RRC_user_Max] AS KPIValue("Connected_RRC_user_Max")
            MEMBER [Measures].[DKD5G_NR] AS KPIValue("DKD5G_NR")
            MEMBER [Measures].[EN_DC_SR_NR] AS KPIValue("EN_DC_SR_NR")
            MEMBER [Measures].[HOSR_NR] AS KPIValue("HOSR_NR")
            MEMBER [Measures].[RASR_NR] AS KPIValue("RASR_NR")
            MEMBER [Measures].[DL_TRAFFIC_NR] AS KPIValue("DL_TRAFFIC_NR")
            MEMBER [Measures].[UL_TRAFFIC_NR] AS KPIValue("UL_TRAFFIC_NR")
            MEMBER [Measures].[User_DL_THP_NR] AS KPIValue("User_DL_THP_NR")
            MEMBER [Measures].[User_UL_THP_NR] AS KPIValue("User_UL_THP_NR")
            MEMBER [Measures].[Cell_DL_THP_NR] AS KPIValue("Cell_DL_THP_NR")
            MEMBER [Measures].[Cell_UL_THP_NR] AS KPIValue("Cell_UL_THP_NR")
        SELECT
            {
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR],
                [Measures].[SDR_NR],
                [Measures].[PRB_Util_DL_NR],
                [Measures].[PRB_Util_UL_NR],
                [Measures].[Latency_NR],
                [Measures].[PktLossR],
                [Measures].[Connected_RRC_user_average],
                [Measures].[Connected_RRC_user_Max],
                [Measures].[DKD5G_NR],
                [Measures].[EN_DC_SR_NR],
                [Measures].[HOSR_NR],
                [Measures].[RASR_NR],
                [Measures].[DL_TRAFFIC_NR],
                [Measures].[UL_TRAFFIC_NR],
                [Measures].[User_DL_THP_NR],
                [Measures].[User_UL_THP_NR],
                [Measures].[Cell_DL_THP_NR],
                [Measures].[Cell_UL_THP_NR]
            } ON COLUMNS,
            NON EMPTY {
                [DimRan5G].[TTML].[TTML].ALLMEMBERS *
                [Date Time].[Calendar].[Quarter].ALLMEMBERS
            } DIMENSION PROPERTIES MEMBER_CAPTION, MEMBER_UNIQUE_NAME ON ROWS
        FROM (
            SELECT { [Date Time].[Calendar].[Quarter].&['+@quarter_id+'] } ON COLUMNS
            FROM [ran5g]
        )
        CELL PROPERTIES VALUE, BACK_COLOR, FORE_COLOR, FORMATTED_VALUE, FORMAT_STRING, FONT_NAME, FONT_SIZE, FONT_FLAGS
    '')
')





---------------CTKD------------------
EXEC('
    SELECT 
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR]")) AS Cell_UL_THP_NR,
		''CTKD'' AS LEVEL
		INTO STAGING_RAN5G_QUARTER_CTKD
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
            MEMBER [Measures].[PS_CSSR_NR] AS KPIValue("PS_CSSR_NR")
            MEMBER [Measures].[SDR_NR] AS KPIValue("SDR_NR")
            MEMBER [Measures].[PRB_Util_DL_NR] AS KPIValue("PRB_Util_DL_NR")
            MEMBER [Measures].[PRB_Util_UL_NR] AS KPIValue("PRB_Util_UL_NR")
            MEMBER [Measures].[Latency_NR] AS KPIValue("Latency_NR")
            MEMBER [Measures].[PktLossR] AS KPIValue("PktLossR")
            MEMBER [Measures].[Connected_RRC_user_average] AS KPIValue("Connected_RRC_user_average")
            MEMBER [Measures].[Connected_RRC_user_Max] AS KPIValue("Connected_RRC_user_Max")
            MEMBER [Measures].[DKD5G_NR] AS KPIValue("DKD5G_NR")
            MEMBER [Measures].[EN_DC_SR_NR] AS KPIValue("EN_DC_SR_NR")
            MEMBER [Measures].[HOSR_NR] AS KPIValue("HOSR_NR")
            MEMBER [Measures].[RASR_NR] AS KPIValue("RASR_NR")
            MEMBER [Measures].[DL_TRAFFIC_NR] AS KPIValue("DL_TRAFFIC_NR")
            MEMBER [Measures].[UL_TRAFFIC_NR] AS KPIValue("UL_TRAFFIC_NR")
            MEMBER [Measures].[User_DL_THP_NR] AS KPIValue("User_DL_THP_NR")
            MEMBER [Measures].[User_UL_THP_NR] AS KPIValue("User_UL_THP_NR")
            MEMBER [Measures].[Cell_DL_THP_NR] AS KPIValue("Cell_DL_THP_NR")
            MEMBER [Measures].[Cell_UL_THP_NR] AS KPIValue("Cell_UL_THP_NR")
        SELECT
            {
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[CTKDCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR],
                [Measures].[SDR_NR],
                [Measures].[PRB_Util_DL_NR],
                [Measures].[PRB_Util_UL_NR],
                [Measures].[Latency_NR],
                [Measures].[PktLossR],
                [Measures].[Connected_RRC_user_average],
                [Measures].[Connected_RRC_user_Max],
                [Measures].[DKD5G_NR],
                [Measures].[EN_DC_SR_NR],
                [Measures].[HOSR_NR],
                [Measures].[RASR_NR],
                [Measures].[DL_TRAFFIC_NR],
                [Measures].[UL_TRAFFIC_NR],
                [Measures].[User_DL_THP_NR],
                [Measures].[User_UL_THP_NR],
                [Measures].[Cell_DL_THP_NR],
                [Measures].[Cell_UL_THP_NR]
            } ON COLUMNS,
            NON EMPTY {
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS *
                [Date Time].[Calendar].[Quarter].ALLMEMBERS
            } DIMENSION PROPERTIES MEMBER_CAPTION, MEMBER_UNIQUE_NAME ON ROWS
        FROM (
            SELECT { [Date Time].[Calendar].[Quarter].&['+@quarter_id+'] } ON COLUMNS
            FROM [ran5g]
        )
        CELL PROPERTIES VALUE, BACK_COLOR, FORE_COLOR, FORMATTED_VALUE, FORMAT_STRING, FONT_NAME, FONT_SIZE, FONT_FLAGS
    '')
')




----------------VENDOR--------------

EXEC('
    SELECT 
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTKD,
        CONVERT(NVARCHAR(50), "[Measures].[VendorCaption]") AS Vendor,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
		CAST(NULL AS NVARCHAR(50)) AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR]")) AS Cell_UL_THP_NR,
		''VENDOR'' AS LEVEL
		INTO STAGING_RAN5G_QUARTER_VENDOR
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[VendorCaption] AS [DimRan5G].[Vendor].CURRENTMEMBER.NAME
            MEMBER [Measures].[PS_CSSR_NR] AS KPIValue("PS_CSSR_NR")
            MEMBER [Measures].[SDR_NR] AS KPIValue("SDR_NR")
            MEMBER [Measures].[PRB_Util_DL_NR] AS KPIValue("PRB_Util_DL_NR")
            MEMBER [Measures].[PRB_Util_UL_NR] AS KPIValue("PRB_Util_UL_NR")
            MEMBER [Measures].[Latency_NR] AS KPIValue("Latency_NR")
            MEMBER [Measures].[PktLossR] AS KPIValue("PktLossR")
            MEMBER [Measures].[Connected_RRC_user_average] AS KPIValue("Connected_RRC_user_average")
            MEMBER [Measures].[Connected_RRC_user_Max] AS KPIValue("Connected_RRC_user_Max")
            MEMBER [Measures].[DKD5G_NR] AS KPIValue("DKD5G_NR")
            MEMBER [Measures].[EN_DC_SR_NR] AS KPIValue("EN_DC_SR_NR")
            MEMBER [Measures].[HOSR_NR] AS KPIValue("HOSR_NR")
            MEMBER [Measures].[RASR_NR] AS KPIValue("RASR_NR")
            MEMBER [Measures].[DL_TRAFFIC_NR] AS KPIValue("DL_TRAFFIC_NR")
            MEMBER [Measures].[UL_TRAFFIC_NR] AS KPIValue("UL_TRAFFIC_NR")
            MEMBER [Measures].[User_DL_THP_NR] AS KPIValue("User_DL_THP_NR")
            MEMBER [Measures].[User_UL_THP_NR] AS KPIValue("User_UL_THP_NR")
            MEMBER [Measures].[Cell_DL_THP_NR] AS KPIValue("Cell_DL_THP_NR")
            MEMBER [Measures].[Cell_UL_THP_NR] AS KPIValue("Cell_UL_THP_NR")
        SELECT
            {
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[VendorCaption],
                [Measures].[PS_CSSR_NR],
                [Measures].[SDR_NR],
                [Measures].[PRB_Util_DL_NR],
                [Measures].[PRB_Util_UL_NR],
                [Measures].[Latency_NR],
                [Measures].[PktLossR],
                [Measures].[Connected_RRC_user_average],
                [Measures].[Connected_RRC_user_Max],
                [Measures].[DKD5G_NR],
                [Measures].[EN_DC_SR_NR],
                [Measures].[HOSR_NR],
                [Measures].[RASR_NR],
                [Measures].[DL_TRAFFIC_NR],
                [Measures].[UL_TRAFFIC_NR],
                [Measures].[User_DL_THP_NR],
                [Measures].[User_UL_THP_NR],
                [Measures].[Cell_DL_THP_NR],
                [Measures].[Cell_UL_THP_NR]
            } ON COLUMNS,
            NON EMPTY {
                [DimRan5G].[Vendor].[Vendor].ALLMEMBERS *
                [Date Time].[Calendar].[Quarter].ALLMEMBERS
            } DIMENSION PROPERTIES MEMBER_CAPTION, MEMBER_UNIQUE_NAME ON ROWS
        FROM (
            SELECT { [Date Time].[Calendar].[Quarter].&['+@quarter_id+'] } ON COLUMNS
            FROM [ran5g]
        )
        CELL PROPERTIES VALUE, BACK_COLOR, FORE_COLOR, FORMATTED_VALUE, FORMAT_STRING, FONT_NAME, FONT_SIZE, FONT_FLAGS
    '')
')




----------------TOANMANG--------------

EXEC('
    SELECT 
        CONVERT(NVARCHAR(50), "[Measures].[QuarterCaption]") AS Quarter,
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTLD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
        CAST(NULL AS NVARCHAR(50)) AS TVT,
        CAST(NULL AS NVARCHAR(50)) AS DVT,
        CAST(NULL AS NVARCHAR(50)) AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR]")) AS Cell_UL_THP_NR,
		''TOANMANG'' AS LEVEL
		INTO STAGING_RAN5G_QUARTER_TOANMANG
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[QuarterCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.NAME
            MEMBER [Measures].[PS_CSSR_NR] AS KPIValue("PS_CSSR_NR")
            MEMBER [Measures].[SDR_NR] AS KPIValue("SDR_NR")
            MEMBER [Measures].[PRB_Util_DL_NR] AS KPIValue("PRB_Util_DL_NR")
            MEMBER [Measures].[PRB_Util_UL_NR] AS KPIValue("PRB_Util_UL_NR")
            MEMBER [Measures].[Latency_NR] AS KPIValue("Latency_NR")
            MEMBER [Measures].[PktLossR] AS KPIValue("PktLossR")
            MEMBER [Measures].[Connected_RRC_user_average] AS KPIValue("Connected_RRC_user_average")
            MEMBER [Measures].[Connected_RRC_user_Max] AS KPIValue("Connected_RRC_user_Max")
            MEMBER [Measures].[DKD5G_NR] AS KPIValue("DKD5G_NR")
            MEMBER [Measures].[EN_DC_SR_NR] AS KPIValue("EN_DC_SR_NR")
            MEMBER [Measures].[HOSR_NR] AS KPIValue("HOSR_NR")
            MEMBER [Measures].[RASR_NR] AS KPIValue("RASR_NR")
            MEMBER [Measures].[DL_TRAFFIC_NR] AS KPIValue("DL_TRAFFIC_NR")
            MEMBER [Measures].[UL_TRAFFIC_NR] AS KPIValue("UL_TRAFFIC_NR")
            MEMBER [Measures].[User_DL_THP_NR] AS KPIValue("User_DL_THP_NR")
            MEMBER [Measures].[User_UL_THP_NR] AS KPIValue("User_UL_THP_NR")
            MEMBER [Measures].[Cell_DL_THP_NR] AS KPIValue("Cell_DL_THP_NR")
            MEMBER [Measures].[Cell_UL_THP_NR] AS KPIValue("Cell_UL_THP_NR")
        SELECT
            {
                [Measures].[QuarterCaption],
                [Measures].[YearCaption],
                [Measures].[PS_CSSR_NR],
                [Measures].[SDR_NR],
                [Measures].[PRB_Util_DL_NR],
                [Measures].[PRB_Util_UL_NR],
                [Measures].[Latency_NR],
                [Measures].[PktLossR],
                [Measures].[Connected_RRC_user_average],
                [Measures].[Connected_RRC_user_Max],
                [Measures].[DKD5G_NR],
                [Measures].[EN_DC_SR_NR],
                [Measures].[HOSR_NR],
                [Measures].[RASR_NR],
                [Measures].[DL_TRAFFIC_NR],
                [Measures].[UL_TRAFFIC_NR],
                [Measures].[User_DL_THP_NR],
                [Measures].[User_UL_THP_NR],
                [Measures].[Cell_DL_THP_NR],
                [Measures].[Cell_UL_THP_NR]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Quarter].ALLMEMBERS
            } DIMENSION PROPERTIES MEMBER_CAPTION, MEMBER_UNIQUE_NAME ON ROWS
        FROM (
            SELECT { [Date Time].[Calendar].[Quarter].&['+@quarter_id+'] } ON COLUMNS
            FROM [ran5g]
        )
        CELL PROPERTIES VALUE, BACK_COLOR, FORE_COLOR, FORMATTED_VALUE, FORMAT_STRING, FONT_NAME, FONT_SIZE, FONT_FLAGS
    '')
')





			
		DELETE FROM MART_RAN5G_REPORT_QUARTERLY WHERE Quarter = @quarter_id;

		INSERT INTO MART_RAN5G_REPORT_QUARTERLY 
		SELECT * FROM(
		SELECT * FROM  STAGING_RAN5G_QUARTER_DISTRICT WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_QUARTER_PROVINCE WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_QUARTER_BRANCH WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_QUARTER_CTKD WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_QUARTER_TTML WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_QUARTER_VENDOR WHERE VENDOR <> 'SAMSUNG' AND DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_QUARTER_DVT WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_QUARTER_TVT WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_QUARTER_TOANMANG WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0) TEMP


		DROP TABLE IF EXISTS STAGING_RAN5G_QUARTER_DISTRICT;
		DROP TABLE IF EXISTS STAGING_RAN5G_QUARTER_PROVINCE;
		DROP TABLE IF EXISTS STAGING_RAN5G_QUARTER_BRANCH;
		DROP TABLE IF EXISTS STAGING_RAN5G_QUARTER_CTKD;
		DROP TABLE IF EXISTS STAGING_RAN5G_QUARTER_TTML;
		DROP TABLE IF EXISTS STAGING_RAN5G_QUARTER_VENDOR;
		DROP TABLE IF EXISTS STAGING_RAN5G_QUARTER_DVT;
		DROP TABLE IF EXISTS STAGING_RAN5G_QUARTER_TVT;
		DROP TABLE IF EXISTS STAGING_RAN5G_QUARTER_TOANMANG;

END

GO

/****** Object:  StoredProcedure [dbo].[RAN_5G_WEEK_GENERAL]    Script Date: 5/19/2025 10:23:17 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[RAN_5G_WEEK_GENERAL] 
	@Ngay varchar(100) = '',
	@last int = 1
AS
BEGIN
	SET NOCOUNT ON;

	SET @ngay = COALESCE(NULLIF(@ngay, ''), CONVERT(VARCHAR(10), GETDATE()-@last, 112));
	
	DECLARE @ngay_103 varchar(10) = CONVERT(VARCHAR(10), convert(datetime, @ngay),103)

	DECLARE @datetime_id int = convert(int, @ngay) * 100

	DECLARE @week_id int = (SELECT TOP (1) WEEK_ID FROM DIM_DATE_TIME WHERE DATE_ID = @ngay)


	DROP TABLE IF EXISTS STAGING_RAN5G_WEEK_DISTRICT;
	DROP TABLE IF EXISTS STAGING_RAN5G_WEEK_PROVINCE;
	DROP TABLE IF EXISTS STAGING_RAN5G_WEEK_BRANCH;
	DROP TABLE IF EXISTS STAGING_RAN5G_WEEK_CTKD;
	DROP TABLE IF EXISTS STAGING_RAN5G_WEEK_TTML;
	DROP TABLE IF EXISTS STAGING_RAN5G_WEEK_VENDOR;
	DROP TABLE IF EXISTS STAGING_RAN5G_WEEK_DVT;
	DROP TABLE IF EXISTS STAGING_RAN5G_WEEK_TVT;
	DROP TABLE IF EXISTS STAGING_RAN5G_WEEK_TOANMANG;

----------DISTRICT---------------------
EXEC('
    SELECT 
		'+@week_id+' AS WEEK_ID,
        RIGHT('+@week_id+', 2) AS Week,
        LEFT('+@week_id+', 4) AS Year,
        CONVERT(NVARCHAR(50), "[Measures].[DistrictCaption]") AS District,
        CONVERT(NVARCHAR(50), "[Measures].[ProvinceCaption]") AS Province,
        CONVERT(NVARCHAR(50), "[Measures].[BRANCHCaption]") AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
        CONVERT(NVARCHAR(50), "[Measures].[TVTCaption]") AS TVT,
        CONVERT(NVARCHAR(50), "[Measures].[DVTCaption]") AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''DISTRICT'' AS LEVEL
		INTO STAGING_RAN5G_WEEK_DISTRICT
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[WeekCaption] AS [Date Time].[Weeks].[Week].CURRENTMEMBER.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.PARENT.NAME
            MEMBER [Measures].[DistrictCaption] AS [DimRan5G].[District].CURRENTMEMBER.NAME
            MEMBER [Measures].[ProvinceCaption] AS [DimRan5G].[Province].CURRENTMEMBER.NAME
            MEMBER [Measures].[BRANCHCaption] AS [DimRan5G].[Chi Nhanh].CURRENTMEMBER.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
            MEMBER [Measures].[TVTCaption] AS [DimRan5G].[TVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[DVTCaption] AS [DimRan5G].[DVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[WeekCaption],
                [Measures].[YearCaption],
                [Measures].[DistrictCaption],
                [Measures].[ProvinceCaption],
                [Measures].[BRANCHCaption],
                [Measures].[CTKDCaption],
                [Measures].[TVTCaption],
                [Measures].[DVTCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Weeks].[Week].&['+@week_id+'] *
                [DimRan5G].[District].[District].ALLMEMBERS *
                [DimRan5G].[Province].[Province].ALLMEMBERS *
                [DimRan5G].[Chi Nhanh].[Chi Nhanh].ALLMEMBERS *
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS *
                [DimRan5G].[TVT].[TVT].ALLMEMBERS *
                [DimRan5G].[DVT].[DVT].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')



---------------PROVINCE----------------
EXEC('
    SELECT 
		'+@week_id+' AS WEEK_ID,
        RIGHT('+@week_id+', 2) AS Week,
        LEFT('+@week_id+', 4) AS Year,
        CAST(NULL AS NVARCHAR(50)) AS District,
        CONVERT(NVARCHAR(50), "[Measures].[ProvinceCaption]") AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''PROVINCE'' AS LEVEL
		INTO STAGING_RAN5G_WEEK_PROVINCE
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[WeekCaption] AS [Date Time].[Weeks].[Week].CURRENTMEMBER.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.PARENT.NAME
            MEMBER [Measures].[ProvinceCaption] AS [DimRan5G].[Province].CURRENTMEMBER.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[WeekCaption],
                [Measures].[YearCaption],
                [Measures].[ProvinceCaption],
                [Measures].[CTKDCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Weeks].[Week].&['+@week_id+'] *
                [DimRan5G].[Province].[Province].ALLMEMBERS *
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')



----------------------BRANCH----------------------------


EXEC('
    SELECT 
		'+@week_id+' AS WEEK_ID,
        RIGHT('+@week_id+', 2) AS Week,
        LEFT('+@week_id+', 4) AS Year,
        CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
        CONVERT(NVARCHAR(50), "[Measures].[BRANCHCaption]") AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''BRANCH'' AS LEVEL
		INTO STAGING_RAN5G_WEEK_BRANCH
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[WeekCaption] AS [Date Time].[Weeks].[Week].CURRENTMEMBER.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.PARENT.NAME
            MEMBER [Measures].[BRANCHCaption] AS [DimRan5G].[Chi Nhanh].CURRENTMEMBER.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[WeekCaption],
                [Measures].[YearCaption],
                [Measures].[BRANCHCaption],
                [Measures].[CTKDCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Weeks].[Week].&['+@week_id+'] *
                [DimRan5G].[Chi Nhanh].[Chi Nhanh].ALLMEMBERS *
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')


-------------------CTKD-------------------
EXEC('
    SELECT 
		'+@week_id+' AS WEEK_ID,
        RIGHT('+@week_id+', 2) AS Week,
        LEFT('+@week_id+', 4) AS Year,
        CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''CTKD'' AS LEVEL
		INTO STAGING_RAN5G_WEEK_CTKD
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[WeekCaption] AS [Date Time].[Weeks].[Week].CURRENTMEMBER.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.PARENT.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[WeekCaption],
                [Measures].[YearCaption],
                [Measures].[CTKDCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Weeks].[Week].&['+@week_id+'] *
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')

----------------TTML------------------

EXEC('
    SELECT 
		'+@week_id+' AS WEEK_ID,
        RIGHT('+@week_id+', 2) AS Week,
        LEFT('+@week_id+', 4) AS Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''TTML'' AS LEVEL
		INTO STAGING_RAN5G_WEEK_TTML
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[WeekCaption] AS [Date Time].[Weeks].[Week].CURRENTMEMBER.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.PARENT.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[WeekCaption],
                [Measures].[YearCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Weeks].[Week].&['+@week_id+'] *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')

----------------------TVT--------------------

EXEC('
    SELECT 
		'+@week_id+' AS WEEK_ID,
        RIGHT('+@week_id+', 2) AS Week,
        LEFT('+@week_id+', 4) AS Year,
        CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTLD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
        CONVERT(NVARCHAR(50), "[Measures].[TVTCaption]") AS TVT,
        CONVERT(NVARCHAR(50), "[Measures].[DVTCaption]") AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''TVT'' AS LEVEL
		INTO STAGING_RAN5G_WEEK_TVT
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[WeekCaption] AS [Date Time].[Weeks].[Week].CURRENTMEMBER.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.PARENT.NAME
            MEMBER [Measures].[TVTCaption] AS [DimRan5G].[TVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[DVTCaption] AS [DimRan5G].[DVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[WeekCaption],
                [Measures].[YearCaption],
                [Measures].[TVTCaption],
                [Measures].[DVTCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Weeks].[Week].&['+@week_id+'] *
                [DimRan5G].[TVT].[TVT].ALLMEMBERS *
                [DimRan5G].[DVT].[DVT].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')


------------------DVT----------------------
EXEC('
    SELECT 
		'+@week_id+' AS WEEK_ID,
        RIGHT('+@week_id+', 2) AS Week,
        LEFT('+@week_id+', 4) AS Year,
        CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
        CONVERT(NVARCHAR(50), "[Measures].[DVTCaption]") AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''DVT'' AS LEVEL
		INTO STAGING_RAN5G_WEEK_DVT
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[WeekCaption] AS [Date Time].[Weeks].[Week].CURRENTMEMBER.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.PARENT.NAME
            MEMBER [Measures].[DVTCaption] AS [DimRan5G].[DVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[WeekCaption],
                [Measures].[YearCaption],
                [Measures].[DVTCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Weeks].[Week].&['+@week_id+'] *
                [DimRan5G].[DVT].[DVT].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')




-----------------VENDOR-------------------------

EXEC('
    SELECT 
		'+@week_id+' AS WEEK_ID,
        RIGHT('+@week_id+', 2) AS Week,
        LEFT('+@week_id+', 4) AS Year,
        CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTKD,
        CONVERT(NVARCHAR(50), "[Measures].[VendorCaption]") AS Vendor,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
		CAST(NULL AS NVARCHAR(50)) AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''VENDOR'' AS LEVEL
		INTO STAGING_RAN5G_WEEK_VENDOR
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[WeekCaption] AS [Date Time].[Weeks].[Week].CURRENTMEMBER.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.PARENT.NAME
            MEMBER [Measures].[VendorCaption] AS [DimRan5G].[Vendor].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[WeekCaption],
                [Measures].[YearCaption],
                [Measures].[VendorCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Weeks].[Week].&['+@week_id+'] *
                [DimRan5G].[Vendor].[Vendor].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')



----------------TTML------------------

EXEC('
    SELECT 
		'+@week_id+' AS WEEK_ID,
        RIGHT('+@week_id+', 2) AS Week,
        LEFT('+@week_id+', 4) AS Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CAST(NULL AS NVARCHAR(50)) AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''TOANMANG'' AS LEVEL
		INTO STAGING_RAN5G_WEEK_TOANMANG
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[WeekCaption] AS [Date Time].[Weeks].[Week].CURRENTMEMBER.NAME
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.PARENT.PARENT.PARENT.NAME
        SELECT
            {
                [Measures].[WeekCaption],
                [Measures].[YearCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Weeks].[Week].&['+@week_id+'] 
            } ON ROWS
        FROM ran5g'')
')


		
		DELETE FROM MART_RAN5G_REPORT_WEEKLY WHERE WEEK_ID = @week_id;

		INSERT INTO MART_RAN5G_REPORT_WEEKLY
		SELECT * FROM(
		SELECT * FROM  STAGING_RAN5G_WEEK_DISTRICT WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_WEEK_PROVINCE WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_WEEK_BRANCH WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_WEEK_CTKD WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_WEEK_TTML WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_WEEK_VENDOR WHERE VENDOR <> 'SAMSUNG' AND DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_WEEK_DVT WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_WEEK_TVT WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_WEEK_TOANMANG WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0) TEMP


		DROP TABLE IF EXISTS STAGING_RAN5G_WEEK_DISTRICT;
		DROP TABLE IF EXISTS STAGING_RAN5G_WEEK_PROVINCE;
		DROP TABLE IF EXISTS STAGING_RAN5G_WEEK_BRANCH;
		DROP TABLE IF EXISTS STAGING_RAN5G_WEEK_CTKD;
		DROP TABLE IF EXISTS STAGING_RAN5G_WEEK_TTML;
		DROP TABLE IF EXISTS STAGING_RAN5G_WEEK_VENDOR;
		DROP TABLE IF EXISTS STAGING_RAN5G_WEEK_DVT;
		DROP TABLE IF EXISTS STAGING_RAN5G_WEEK_TVT;
		DROP TABLE IF EXISTS STAGING_RAN5G_WEEK_TOANMANG;
END

GO

/****** Object:  StoredProcedure [dbo].[RAN_5G_YEAR_GENERAL]    Script Date: 5/19/2025 10:23:17 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[RAN_5G_YEAR_GENERAL]
	@Ngay varchar(100) = '',
	@last int = 1
AS
BEGIN
	SET NOCOUNT ON;

	SET @ngay = COALESCE(NULLIF(@ngay, ''), CONVERT(VARCHAR(10), GETDATE()-@last, 112));
	
	DECLARE @ngay_103 varchar(10) = CONVERT(VARCHAR(10), convert(datetime, @ngay),103)

	DECLARE @datetime_id int = convert(int, @ngay) * 100

	DECLARE @year_id int = (SELECT TOP (1) YEAR_ID FROM DIM_DATE_TIME WHERE DATE_ID = @ngay)


	DROP TABLE IF EXISTS STAGING_RAN5G_YEAR_DISTRICT;
	DROP TABLE IF EXISTS STAGING_RAN5G_YEAR_PROVINCE;
	DROP TABLE IF EXISTS STAGING_RAN5G_YEAR_BRANCH;
	DROP TABLE IF EXISTS STAGING_RAN5G_YEAR_CTKD;
	DROP TABLE IF EXISTS STAGING_RAN5G_YEAR_TTML;
	DROP TABLE IF EXISTS STAGING_RAN5G_YEAR_VENDOR;
	DROP TABLE IF EXISTS STAGING_RAN5G_YEAR_DVT;
	DROP TABLE IF EXISTS STAGING_RAN5G_YEAR_TVT;
	DROP TABLE IF EXISTS STAGING_RAN5G_YEAR_TOANMANG;


-----------DISTRICT--------------------

EXEC('
    SELECT 
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CONVERT(NVARCHAR(50), "[Measures].[DistrictCaption]") AS District,
        CONVERT(NVARCHAR(50), "[Measures].[ProvinceCaption]") AS Province,
        CONVERT(NVARCHAR(50), "[Measures].[ChiNhanhCaption]") AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
        CONVERT(NVARCHAR(50), "[Measures].[TVTCaption]") AS TVT,
        CONVERT(NVARCHAR(50), "[Measures].[DVTCaption]") AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''DISTRICT'' AS LEVEL
		INTO STAGING_RAN5G_YEAR_DISTRICT
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[DistrictCaption] AS [DimRan5G].[DISTRICT].CURRENTMEMBER.NAME
            MEMBER [Measures].[ProvinceCaption] AS [DimRan5G].[PROVINCE].CURRENTMEMBER.NAME
            MEMBER [Measures].[ChiNhanhCaption] AS [DimRan5G].[Chi Nhanh].CURRENTMEMBER.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
            MEMBER [Measures].[TVTCaption] AS [DimRan5G].[TVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[DVTCaption] AS [DimRan5G].[DVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[YearCaption],
				[Measures].[DistrictCaption],
                [Measures].[ProvinceCaption],
                [Measures].[ChiNhanhCaption],
                [Measures].[CTKDCaption],
                [Measures].[TVTCaption],
                [Measures].[DVTCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Year].&['+@year_id+'] *
				[DimRan5G].[DISTRICT].[DISTRICT].ALLMEMBERS *
                [DimRan5G].[PROVINCE].[PROVINCE].ALLMEMBERS *
                [DimRan5G].[Chi Nhanh].[Chi Nhanh].ALLMEMBERS *
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS *
                [DimRan5G].[TVT].[TVT].ALLMEMBERS *
                [DimRan5G].[DVT].[DVT].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')



-----------PROVINCE--------------------

EXEC('
    SELECT 
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CAST(NULL AS NVARCHAR(50)) AS District,
        CONVERT(NVARCHAR(50), "[Measures].[ProvinceCaption]") AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''PROVINCE'' AS LEVEL
		INTO STAGING_RAN5G_YEAR_PROVINCE
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[ProvinceCaption] AS [DimRan5G].[PROVINCE].CURRENTMEMBER.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[YearCaption],
                [Measures].[ProvinceCaption],
                [Measures].[CTKDCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Year].&['+@year_id+'] *
                [DimRan5G].[PROVINCE].[PROVINCE].ALLMEMBERS *
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')



-----------CHI_NHANH--------------------

EXEC('
    SELECT 
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
        CONVERT(NVARCHAR(50), "[Measures].[ChiNhanhCaption]") AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''BRANCH'' AS LEVEL
		INTO STAGING_RAN5G_YEAR_BRANCH
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[ChiNhanhCaption] AS [DimRan5G].[Chi Nhanh].CURRENTMEMBER.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[YearCaption],
                [Measures].[ChiNhanhCaption],
                [Measures].[CTKDCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Year].&['+@year_id+'] *
                [DimRan5G].[Chi Nhanh].[Chi Nhanh].ALLMEMBERS *
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')


-----------TVT--------------------

EXEC('
    SELECT 
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTLD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
        CONVERT(NVARCHAR(50), "[Measures].[TVTCaption]") AS TVT,
        CONVERT(NVARCHAR(50), "[Measures].[DVTCaption]") AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''TVT'' AS LEVEL
		INTO STAGING_RAN5G_YEAR_TVT
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[TVTCaption] AS [DimRan5G].[TVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[DVTCaption] AS [DimRan5G].[DVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[YearCaption],
                [Measures].[TVTCaption],
                [Measures].[DVTCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Year].&['+@year_id+'] *
                [DimRan5G].[TVT].[TVT].ALLMEMBERS *
                [DimRan5G].[DVT].[DVT].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')


-----------DVT--------------------

EXEC('
    SELECT 
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
        CAST(NULL AS NVARCHAR(50)) AS TVT,
        CONVERT(NVARCHAR(50), "[Measures].[DVTCaption]") AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''DVT'' AS LEVEL
		INTO STAGING_RAN5G_YEAR_DVT
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[DVTCaption] AS [DimRan5G].[DVT].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[YearCaption],
                [Measures].[DVTCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Year].&['+@year_id+'] *
                [DimRan5G].[DVT].[DVT].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')











-----------TTML--------------------

EXEC('
    SELECT 
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
        CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''TTML'' AS LEVEL
		INTO STAGING_RAN5G_YEAR_TTML
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[YearCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Year].&['+@year_id+'] *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')





-----------CTKD--------------------

EXEC('
    SELECT 
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
        CONVERT(NVARCHAR(50), "[Measures].[CTKDCaption]") AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CONVERT(NVARCHAR(50), "[Measures].[TTMLCaption]") AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''CTKD'' AS LEVEL
		INTO STAGING_RAN5G_YEAR_CTKD
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[CTKDCaption] AS [DimRan5G].[CTKD].CURRENTMEMBER.NAME
            MEMBER [Measures].[TTMLCaption] AS [DimRan5G].[TTML].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[YearCaption],
                [Measures].[CTKDCaption],
                [Measures].[TTMLCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Year].&['+@year_id+'] *
                [DimRan5G].[CTKD].[CTKD].ALLMEMBERS *
                [DimRan5G].[TTML].[TTML].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')



-------------VENDOR----------------

EXEC('
    SELECT 
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
        CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTKD,
        CONVERT(NVARCHAR(50), "[Measures].[VendorCaption]") AS Vendor,
		CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
		CAST(NULL AS NVARCHAR(50)) AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''VENDOR'' AS LEVEL
		INTO STAGING_RAN5G_YEAR_VENDOR
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
            MEMBER [Measures].[VendorCaption] AS [DimRan5G].[Vendor].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[YearCaption],
                [Measures].[VendorCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Year].&['+@year_id+'] *
                [DimRan5G].[Vendor].[Vendor].ALLMEMBERS
            } ON ROWS
        FROM ran5g'')
')





-----------TOANMANG--------------------

EXEC('
    SELECT 
        CONVERT(NVARCHAR(50), "[Measures].[YearCaption]") AS Year,
		CAST(NULL AS NVARCHAR(50)) AS District,
		CAST(NULL AS NVARCHAR(50)) AS Province,
		CAST(NULL AS NVARCHAR(50)) AS Chi_Nhanh,
		CAST(NULL AS NVARCHAR(50)) AS CTKD,
		CAST(NULL AS NVARCHAR(50)) AS VENDOR,
        CAST(NULL AS NVARCHAR(50)) AS TVT,
		CAST(NULL AS NVARCHAR(50)) AS DVT,
        CAST(NULL AS NVARCHAR(50)) AS TTML,
        CONVERT(float, CONVERT(REAL, "[Measures].[PS_CSSR_NR Value]")) AS PS_CSSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[SDR_NR Value]")) AS SDR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_DL_NR Value]")) AS PRB_Util_DL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PRB_Util_UL_NR Value]")) AS PRB_Util_UL_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Latency_NR Value]")) AS Latency_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[PktLossR Value]")) AS PktLossR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_average Value]")) AS Connected_RRC_user_average,
        CONVERT(float, CONVERT(REAL, "[Measures].[Connected_RRC_user_Max Value]")) AS Connected_RRC_user_Max,
        CONVERT(float, CONVERT(REAL, "[Measures].[DKD5G_NR Value]")) AS DKD5G_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[EN_DC_SR_NR Value]")) AS EN_DC_SR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[HOSR_NR Value]")) AS HOSR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[RASR_NR Value]")) AS RASR_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[DL_TRAFFIC_NR Value]")) AS DL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[UL_TRAFFIC_NR Value]")) AS UL_TRAFFIC_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_DL_THP_NR Value]")) AS User_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[User_UL_THP_NR Value]")) AS User_UL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_DL_THP_NR Value]")) AS Cell_DL_THP_NR,
        CONVERT(float, CONVERT(REAL, "[Measures].[Cell_UL_THP_NR Value]")) AS Cell_UL_THP_NR,
		''TOANMANG'' AS LEVEL
		INTO STAGING_RAN5G_YEAR_TOANMANG
    FROM OPENQUERY(RAN5G, ''
        WITH 
            MEMBER [Measures].[YearCaption] AS [Date Time].[Calendar].CURRENTMEMBER.NAME
        SELECT
            {
                [Measures].[YearCaption],
                [Measures].[PS_CSSR_NR Value],
                [Measures].[SDR_NR Value],
                [Measures].[PRB_Util_DL_NR Value],
                [Measures].[PRB_Util_UL_NR Value],
                [Measures].[Latency_NR Value],
                [Measures].[PktLossR Value],
                [Measures].[Connected_RRC_user_average Value],
                [Measures].[Connected_RRC_user_Max Value],
                [Measures].[DKD5G_NR Value],
                [Measures].[EN_DC_SR_NR Value],
                [Measures].[HOSR_NR Value],
                [Measures].[RASR_NR Value],
                [Measures].[DL_TRAFFIC_NR Value],
                [Measures].[UL_TRAFFIC_NR Value],
                [Measures].[User_DL_THP_NR Value],
                [Measures].[User_UL_THP_NR Value],
                [Measures].[Cell_DL_THP_NR Value],
                [Measures].[Cell_UL_THP_NR Value]
            } ON COLUMNS,
            NON EMPTY {
                [Date Time].[Calendar].[Year].&['+@year_id+'] 
            } ON ROWS
        FROM ran5g'')
')





		DELETE FROM MART_RAN5G_REPORT_YEARLY WHERE Year = @year_id;

		INSERT INTO MART_RAN5G_REPORT_YEARLY 
		SELECT * FROM(
		SELECT * FROM  STAGING_RAN5G_YEAR_DISTRICT WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_YEAR_PROVINCE WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_YEAR_BRANCH WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_YEAR_CTKD WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_YEAR_TTML WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_YEAR_VENDOR WHERE VENDOR <> 'SAMSUNG' AND DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_YEAR_DVT WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_YEAR_TVT WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0
		UNION
		SELECT * FROM  STAGING_RAN5G_YEAR_TOANMANG WHERE DKD5G_NR >0 and (DL_TRAFFIC_NR+UL_TRAFFIC_NR)>0) TEMP


		DROP TABLE IF EXISTS STAGING_RAN5G_YEAR_DISTRICT;
		DROP TABLE IF EXISTS STAGING_RAN5G_YEAR_PROVINCE;
		DROP TABLE IF EXISTS STAGING_RAN5G_YEAR_BRANCH;
		DROP TABLE IF EXISTS STAGING_RAN5G_YEAR_CTKD;
		DROP TABLE IF EXISTS STAGING_RAN5G_YEAR_TTML;
		DROP TABLE IF EXISTS STAGING_RAN5G_YEAR_VENDOR;
		DROP TABLE IF EXISTS STAGING_RAN5G_YEAR_DVT;
		DROP TABLE IF EXISTS STAGING_RAN5G_YEAR_TVT;
		DROP TABLE IF EXISTS STAGING_RAN5G_YEAR_TOANMANG;



END
GO


