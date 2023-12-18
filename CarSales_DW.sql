/*CREATE DATA WAREHOUSE*/
/*create database CarSales_DW;*/
use CarSales_DW;
GO

/* --- DELETE ONE BY ONE OPTION ---*/
IF OBJECT_ID('dbo.FACT_SALESTRANSACTION', 'U') IS NOT NULL  
DROP TABLE FACT_SALESTRANSACTION;
IF OBJECT_ID('dbo.DIM_VEHICLE', 'U') IS NOT NULL 
DROP TABLE DIM_VEHICLE;
IF OBJECT_ID('dbo.DIM_CUSTOMER', 'U') IS NOT NULL 
DROP TABLE DIM_CUSTOMER;
IF OBJECT_ID('dbo.DIM_STAFF', 'U') IS NOT NULL 
DROP TABLE DIM_STAFF;
IF OBJECT_ID('dbo.DIM_BRANCH', 'U') IS NOT NULL 
DROP TABLE DIM_BRANCH;
IF OBJECT_ID('dbo.DIM_DATE', 'U') IS NOT NULL 
DROP TABLE DIM_DATE;
IF OBJECT_ID('dbo.DIM_TIME', 'U') IS NOT NULL 
DROP TABLE DIM_TIME;
--------

/*CREATE TABLES*/
CREATE TABLE DIM_VEHICLE(
	VEHICLEID INT PRIMARY KEY,
    MAKEID INT,
    MODELID INT,
    COSTPRICE DECIMAL(18,2),
    PRICE DECIMAL(18,2),
    QUANTITY INT,
    DATEMANUFACTURED DATE,
    MAKE NVARCHAR(50),
    MODEL NVARCHAR(50),
    BODYSTYLE NVARCHAR(50),
);

CREATE TABLE DIM_CUSTOMER(
	CUSTOMERID	Numeric Primary Key,
	FULLNAME	Nvarchar(60),
	GENDER	NVarchar(6),
	TITLE	Nvarchar(10),
	DOB	Date,
	AGEGROUP NVarchar(15),
	CELLNO	NVarchar(20),
	ADDRESS	NVarchar(250),
	EMAIL	NVarchar(100),
	COUNTRYID	Int,
	COUNTRY	NVarchar(100),
	CONTINENT NVarchar(50),
	DATEREGISTERED	Date,
	STATUSID	Int,
	CUSTOMERSTATUS	NVarchar(50)
);

CREATE TABLE DIM_STAFF(
	STAFFID	Numeric Primary Key,
	FIRSTNAME	NVarchar(30),
	SURNAME	NVarchar(30),
	GENDER	NVarchar(6),
	TITLE	NVarchar(10),
	DOB	Date,
	AGEGROUP NVarchar(15),
	CELLNO	NVarchar(20),
	ADDRESS	NVarchar(250),
	EMAIL	NVarchar(100),
	BRANCHID	Int,
	BRANCHNAME NVarchar(50),
	POSITIONID	int,
	POSITIONNAME	NVarchar(50),
	DATEJOINED	Date,
	STATUSID	Int,
	STAFFSTATUS	NVarchar(50)
);

CREATE TABLE DIM_BRANCH(
	BRANCHID	Int Primary Key,
	BRANCHNAME	NVarchar(50),
	ADDRESS	NVarchar(250),
	COUNTRYID	Int,
	COUNTRY	NVarchar(100),
	CONTINENT	NVarchar(50),
	EMAIL	NVarchar(100),
	TELNO	NVarchar(50)
);



CREATE TABLE DIM_TIME(
	TIMEKEY	Numeric Primary Key,
	TIMEFROM Time(7),
	TIMETO	Time(7),
	[HOUR]	Int,
	AMPM	NVarchar(2)
);

CREATE TABLE DIM_DATE(
	DATEKEY	Numeric Primary Key,
	[DATE]	Date,
	[DAY]	Int,
	[WEEKDAY]	Int,
	WEEKDAYNAME	NVarchar(10),
	SHORTWEEKDAYNAME	NVarchar(4),
	[DAYOFYEAR]	Int,
	WEEKOFMONTH	Int,
	WEEKOFYEAR	Int,
	[MONTH]	int,
	[MONTHNAME]	NVarchar(10),
	SHORTMONTHNAME	NVarchar(3),
	SEASON	NVarchar(10),
	[QUARTER]	Int,
	QUARTERNAME	NVarchar(10),
	[YEAR]	Int,
	MMYYYY Int,
	MONTHYEAR NVarchar(7),
	ISWEEKEND	Int,
	ISHOLIDAY	Int,
	HOLIDAYNAME	NVarchar(100),
	SPECIALDAYS NVarchar(100),
	FINANCIALYEAR	Int,
	FINANCIALQUARTER Int,
	FINANCIALMONTH	Int,
	FIRSTDATEOFYEAR	Date,
	LASTDATEOFYEAR	Date,
	FIRSTDATEOFQUARTER	Date,
	LASTDATEOFQUARTER	Date,
	FIRSTDATEOFMONTH	Date,
	LASTDATEOFMONTH	Date,
	FIRSTDATEOFWEEK Date,
	LASTDATEOFWEEK Date
);

CREATE TABLE FACT_SALESTRANSACTION(
	TRANID	Numeric Primary Key,
	DATEKEY	Numeric,
	TRANDATE	Date,
	TIMEKEY	Numeric,
	TRANTIME	Time,
	CUSTOMERID	Numeric,-- Foreign Key References DIM_CUSTOMER(CUSTOMERID),
	VEHICLEID	Int,-- Foreign Key References DIM_VEHICLE(VEHICLEID),
	PRICE	Decimal(18,2),
	QUANTITY	Int,
	AMOUNT	Decimal(18,2),
	BRANCHID	Int,-- 
	STAFFID	Numeric,-- Foreign Key References DIM_STAFF(STAFFID),
	STATUSID	Int,
	TRANSACTIONSTATUS	NVarchar(50)

);
/*End Script*/



/*----------------DIM_VEHICLE ETL Code----------------*/
use CarSales_DW;  --specify target table database for truncating--
TRUNCATE TABLE DIM_VEHICLE;
use CarSales_DB;  --specify source table database--

--increment variables--
DECLARE @RowCount INT;
DECLARE @RowCounter INT = 1;
--column variables--
DECLARE @Vehicleid int ;
declare @Makeid int;
declare @Modelid int;
declare @Costprice decimal(18,2);
declare @Price decimal(18,2);
declare @Quantity int;
declare @DateManufactured date;
declare @Make nvarchar(50);
declare @Model nvarchar(50);
declare @Bodystyle nvarchar(50);

--get total rows in source table-- 
SELECT @RowCount = COUNT(*) FROM VEHICLE;

WHILE @RowCounter <= @RowCount
--WHILE (select count(*) from Table) > @Counter) --one liner option

BEGIN

use CarSales_DB; --specify source table database--

   --EXTRACTION-- GET DATA INTO CTE ASSIGNING ROWID FOR FILTERING--
   WITH CTE AS(
	   SELECT ROW_NUMBER() OVER ( ORDER BY a.VEHICLEID ) AS ROWID, 
			a.*, 
			 b.MAKE,b.DESCRIPTION AS BRANDDESCRIPTION, 
			c.BODYSTYLE, c.MODEL AS MODELDESCRIPTION
	   FROM VEHICLE a,
			MAKE b,
			MODEL c
		WHERE a.MAKEID = b.MAKEID AND 
			  a.MODELID= c.MODELID
   )

   --PERFORM OPERATIONS : TRANSFORM
   --TRANSFORMATION-- EXTRACT FROM CTE INTO VARIABLES 
   SELECT @VehicleID       = a.VEHICLEID, 
		  @MakeID		   = a.MAKEID,
		  @ModelID	       = a.MODELID,
		  @CostPrice	   = a.COSTPRICE, 
		  @Price		   = a.PRICE,
		  @Quantity		   = a.QUANTITY,
		  @DateManufactured= a.DATEMANUFACTURED, 
		  @Make	           = IsNull(a.BRANDDESCRIPTION,''),
		  @Model           = IsNull(a.MODELDESCRIPTION,''),
		  @Bodystyle       = Isnull(a.BODYSTYLE,'')
		  
	 FROM CTE a
      WHERE a.ROWID = @RowCounter --use ROWID criteria to get CurrentRow
      ORDER BY a.VEHICLEID ASC;
		 
	--PERFORM OPERATIONS : LOAD
	use CarSales_DW;  --specify source table database--

	INSERT INTO DIM_VEHICLE VALUES( @VehicleID, @MakeID, @ModelID, @CostPrice,  @Price, @Quantity, @DateManufactured, @Make, @Model,  @Bodystyle);
 
   --increment counter -- avoid infinite loop --
   SET @RowCounter += 1;
   
END

PRINT '--- Extract Transform and Load ETL Process for DIM_PRODUCT Completed Successfully ---';
GO

/*--------------End Processing---------------*/


/*----------------DIM_CUSTOMER ETL Code----------------*/
use CarSales_DW;  --specify target table database for truncating--
TRUNCATE TABLE DIM_CUSTOMER;
use CarSales_DB;  --specify source table database--

--increment variables--
DECLARE @RowCount INT;
DECLARE @RowCounter INT = 1;
--column variables--
DECLARE @CustomerID INT;
DECLARE @Fullname VARCHAR(60);
DECLARE @Gender VARCHAR(6);
DECLARE @Title VARCHAR(10);
DECLARE @DOB Date;
DECLARE @AgeGroup VARCHAR(15);
DECLARE @CellNO VARCHAR(20);
DECLARE @Address VARCHAR(250);
DECLARE @Email VARCHAR(100);
DECLARE @CountryID INT;
DECLARE @Country VARCHAR(100);
DECLARE @Continent VARCHAR(50);
DECLARE @DateRegistered Date;
DECLARE @StatusID Int;
DECLARE @CustomerStatus VARCHAR(50);

--get total rows in source table-- 
SELECT @RowCount = COUNT(*) FROM CUSTOMER;

WHILE @RowCounter <= @RowCount
--WHILE (select count(*) from Table) > @Counter) --one liner option

BEGIN

use CarSales_DB; --specify source table database--

   --EXTRACTION-- GET DATA INTO CTE ASSIGNING ROWID FOR FILTERING--
   WITH CTE AS(
	   SELECT ROW_NUMBER() OVER ( ORDER BY a.CUSTOMERID ) AS ROWID, 
			a.*, 
			b.CUSTOMERSTATUS, b.DESCRIPTION, 
			c.COUNTRY, c.CONTINENT
	   FROM CUSTOMER a,
			CUSTOMERSTATUS b,
			COUNTRY c
		WHERE a.STATUSID = b.STATUSID AND 
			  a.COUNTRYID= c.COUNTRYID
   )

   --PERFORM OPERATIONS : TRANSFORM
   --TRANSFORMATION-- EXTRACT FROM CTE INTO VARIABLES 
   SELECT @CustomerID    = a.CUSTOMERID, 
		  @Fullname      = a.FIRSTNAME+' '+a.SURNAME, 
		  @Gender	     = CASE WHEN a.Gender='M' THEN 'Male' ELSE 'Female' END, 
		  @Title         = CASE WHEN a.Gender='M'THEN 'Mr.' ELSE 'Ms.' END, 
		  @DOB		     = a.DOB,
		  @AgeGroup      = CASE WHEN DATEDIFF(YEAR, a.DOB, GETDATE()) < 13 THEN 'Child' WHEN DATEDIFF(YEAR, a.DOB, GETDATE()) < 20 THEN 'Teenager' WHEN DATEDIFF(YEAR, a.DOB, GETDATE()) < 36 THEN 'Youth' WHEN DATEDIFF(YEAR, a.DOB, GETDATE()) < 51 THEN 'Adult' WHEN DATEDIFF(YEAR, a.DOB, GETDATE()) < 66 THEN 'Elder'  ELSE 'Senior Citizen' END, 
		  @CellNO        = IsNull(a.CELLNO,''),
		  @Address       = CAST(a.ADDRESS AS NVARCHAR),
		  @Email         = IsNull(a.EMAIL,''),
		  @CountryID     = a.COUNTRYID, 
		  @Country	     = IsNull(a.COUNTRY,''),
		  @Continent     = IsNull(a.CONTINENT,''),
		  @DateRegistered= a.DATEREGISTERED,
		  @StatusID      = a.STATUSID,
		  @CustomerStatus= IsNull(a.CUSTOMERSTATUS,'')
      FROM CTE a
      WHERE a.ROWID = @RowCounter --use ROWID criteria to get CurrentRow
      ORDER BY a.CUSTOMERID ASC;
		 
	--PERFORM OPERATIONS : LOAD
	use CarSales_DW;  --specify source table database--

	INSERT INTO DIM_CUSTOMER VALUES(@CustomerID, @Fullname, @Gender, @Title, @DOB, @AgeGroup, @CellNO, @Address, @Email, @CountryID, @Country, @Continent, @DateRegistered, @StatusID, @CustomerStatus);
 
   --increment counter -- avoid infinite loop --
   SET @RowCounter += 1;
   
END

PRINT '--- Extract Transform and Load ETL Process for DIM_CUSTOMER Completed Successfully ---';
GO
/*--------------End Processing---------------*/





/*----------------DIM_STAFF ETL Code----------------*/
use CarSales_DW;  --specify target table database for truncating--
TRUNCATE TABLE DIM_STAFF;
use CarSales_DB;  --specify source table database--

--increment variables--
DECLARE @RowCount INT;
DECLARE @RowCounter INT = 1;
--column variables--
DECLARE @StaffID INT;
DECLARE @Firstname VARCHAR(30);
DECLARE @Surname VARCHAR(30);
DECLARE @Gender VARCHAR(6);
DECLARE @Title VARCHAR(10);
DECLARE @DOB Date;
DECLARE @AgeGroup VARCHAR(15);
DECLARE @CellNO VARCHAR(20);
DECLARE @Address VARCHAR(250);
DECLARE @Email VARCHAR(100);
DECLARE @BranchID INT;
DECLARE @Branchname VARCHAR(50);
DECLARE @PositionID INT;
DECLARE @PositionName VARCHAR(50);
DECLARE @DateJoined Date;
DECLARE @StatusID Int;
DECLARE @StaffStatus VARCHAR(50);

--get total rows in source table-- 
SELECT @RowCount = COUNT(*) FROM STAFF;

WHILE @RowCounter <= @RowCount
--WHILE (select count(*) from Table) > @Counter) --one liner option

BEGIN

use CarSales_DB; --specify source table database--

   --EXTRACTION-- GET DATA INTO CTE ASSIGNING ROWID FOR FILTERING--
   WITH CTE AS(
	   SELECT ROW_NUMBER() OVER ( ORDER BY a.STAFFID ) AS ROWID, 
			a.*, 
			b.STAFFSTATUS, b.DESCRIPTION, 
			c.BRANCHNAME, 
			d.POSITIONNAME
	   FROM STAFF a,
			STAFFSTATUS b,
			BRANCH c,
			POSITION d
		WHERE a.STATUSID = b.STATUSID AND 
			  a.BRANCHID= c.BRANCHID AND
			  a.POSITIONID=d.POSITIONID
   )

   --PERFORM OPERATIONS : TRANSFORM
   --TRANSFORMATION-- EXTRACT FROM CTE INTO VARIABLES 
   SELECT @StaffID		 = a.STAFFID, 
		  @Firstname     = a.FIRSTNAME, 
		  @Surname       = a.SURNAME, 
		  @Gender	     = CASE WHEN a.Gender='M' THEN 'Male' ELSE 'Female' END, 
		  @Title         = CASE WHEN a.Gender='M'THEN 'Mr.' ELSE 'Ms.' END, 
		  @DOB		     = a.DOB,
		  @AgeGroup      = CASE WHEN DATEDIFF(YEAR, a.DOB, GETDATE()) < 13 THEN 'Child' WHEN DATEDIFF(YEAR, a.DOB, GETDATE()) < 20 THEN 'Teenager' WHEN DATEDIFF(YEAR, a.DOB, GETDATE()) < 36 THEN 'Youth' WHEN DATEDIFF(YEAR, a.DOB, GETDATE()) < 51 THEN 'Adult' WHEN DATEDIFF(YEAR, a.DOB, GETDATE()) < 66 THEN 'Elder'  ELSE 'Senior Citizen' END, 
		  @CellNO        = IsNull(a.CELLNO,''),
		  @Address       = CAST(a.ADDRESS AS NVARCHAR),
		  @Email         = IsNull(a.EMAIL,''),
		  @BranchID     = a.BRANCHID, 
		  @Branchname	     = IsNull(a.BRANCHNAME,''),  
		  @PositionID    = a.POSITIONID, 
		  @PositionName  = IsNull(a.POSITIONNAME,''), 
		  @DateJoined    = a.DATEJOINED,
		  @StatusID      = a.STATUSID,
		  @StaffStatus= IsNull(a.STAFFSTATUS,'')
      FROM CTE a
      WHERE a.ROWID = @RowCounter --use ROWID criteria to get CurrentRow
      ORDER BY a.STAFFID ASC;
		 
	--PERFORM OPERATIONS : LOAD
	use CarSales_DW;  --specify source table database--

	INSERT INTO DIM_STAFF VALUES(@StaffID, @Firstname, @Surname, @Gender, @Title, @DOB, @AgeGroup, @CellNO, @Address, @Email, @BranchID, @Branchname, @PositionID, @PositionName, @DateJoined, @StatusID, @StaffStatus);
 
   --increment counter -- avoid infinite loop --
   SET @RowCounter += 1;
   
END

PRINT '--- Extract Transform and Load ETL Process for DIM_STAFF Completed Successfully ---';
GO
/*--------------End Processing---------------*/


/*----------------DIM_BRANCH ETL Code----------------*/
use CarSales_DW;  --specify target table database for truncating--
TRUNCATE TABLE DIM_BRANCH;
use CarSales_DB;  --specify source table database--

--increment variables--
DECLARE @RowCount INT;
DECLARE @RowCounter INT = 1;
--column variables--
DECLARE @BranchID INT;
DECLARE @Branchname VARCHAR(50);
DECLARE @Address VARCHAR(250);
DECLARE @CountryID INT;
DECLARE @Country VARCHAR(100);
DECLARE @Continent VARCHAR(50);
DECLARE @Email   VARCHAR(100);
DECLARE @TelNO  VARCHAR(50);

--get total rows in source table-- 
SELECT @RowCount = COUNT(*) FROM BRANCH;

WHILE @RowCounter <= @RowCount
--WHILE (select count(*) from Table) > @Counter) --one liner option

BEGIN

use CarSales_DB; --specify source table database--

   --EXTRACTION-- GET DATA INTO CTE ASSIGNING ROWID FOR FILTERING--
   WITH CTE AS(
	   SELECT ROW_NUMBER() OVER ( ORDER BY a.BRANCHID ) AS ROWID, 
			a.*, 
			b.COUNTRY, b.CONTINENT
			
	   FROM BRANCH a,
			COUNTRY b
			
		WHERE a.COUNTRYID  = b.COUNTRYID
		
   )

   --PERFORM OPERATIONS : TRANSFORM
   --TRANSFORMATION-- EXTRACT FROM CTE INTO VARIABLES 
   SELECT @BranchID       = a.BRANCHID, 
		  @Branchname     = a.BRANCHNAME, 
		  @Address	     = a.ADDRESS, 
		  @CountryID     = a.COUNTRYID, 
		  @Country	     = IsNull(a.COUNTRY,''), 
		  @Continent     = IsNull(a.CONTINENT,''), 
		  @Email         = IsNull(a.EMAIL,''),
		  @TelNO        = IsNull(a.TELNO,'')
      FROM CTE a
      WHERE a.ROWID = @RowCounter --use ROWID criteria to get CurrentRow
      ORDER BY a.BRANCHID ASC;
		 
	--PERFORM OPERATIONS : LOAD
	use CarSales_DW;  --specify source table database--

	INSERT INTO DIM_BRANCH VALUES(@BranchID, @Branchname, @Address, @CountryID, @Country, @Continent, @Email, @TelNO);
 
   --increment counter -- avoid infinite loop --
   SET @RowCounter += 1;
   
END

PRINT '--- Extract Transform and Load ETL Process for DIM_STORE Completed Successfully ---';
GO
/*--------------End Processing---------------*/






/*----------------DIM_TIME ETL Code----------------*/
use CarSales_DW;  --specify target table database for truncating--
TRUNCATE TABLE DIM_TIME;
use CarSales_DB;  --specify source table database--

--increment variables--
DECLARE @RowCount INT;
DECLARE @RowCounter INT = 1;
--other variables--
DECLARE @TimeKey INT;
DECLARE @Hour INT;
DECLARE @TimeFrom TIME;
DECLARE @TimeTo TIME;
DECLARE @AmPm VARCHAR(2);

--get total rows in source table-- 
SELECT @RowCount = 24; --total hours in a day since online sales are throughout the day

WHILE @RowCounter <= @RowCount
--WHILE (select count(*) from Table) > @Counter) --one liner option

BEGIN

   --PERFORM OPERATIONS : TRANSFORM
   SELECT @TimeKey		 = @RowCounter, 
		  @Hour		     = @RowCounter, 
		  @TimeFrom      = CAST( CAST(@Hour-1 AS VARCHAR) + ':31' AS TIME),
		  @TimeTo	     = CASE WHEN @Hour = 24 THEN  CAST( CAST(0 AS VARCHAR) + ':30' AS TIME) 
						   ELSE  CAST( CAST(@Hour AS VARCHAR) + ':30' AS TIME)   END,
		  @AmPm          = CASE WHEN @Hour < 12 THEN 'AM' ELSE 'PM' END;
		 
	--PERFORM OPERATIONS : LOAD
	use CarSales_DW;  --specify source table database--

	INSERT INTO DIM_TIME VALUES(@TimeKey, @TimeFrom, @TimeTo, @Hour, @AmPm);
 
   --increment counter -- avoid infinite loop --
   SET @RowCounter += 1;
   
END

PRINT '--- Extract Transform and Load ETL Process for DIM_TIME Completed Successfully ---';
GO
/*----------------DIM_TIME ETL Code END----------------*/






/*----------------DIM_DATE ETL Code----------------*/
use CarSales_DW;  --specify target table database for truncating--
TRUNCATE TABLE DIM_DATE;

DECLARE @CurrentDate DATE = '1980-01-01'; --start date
DECLARE @Interval INT  = 20; --years for forecasting into the future
DECLARE @EndDate  DATE =  CAST(CAST((YEAR(GetDate()) + @Interval) AS VARCHAR(4)) + '-12-31' AS DATE); --current date + @Interval years for forecasting

----
WHILE @CurrentDate < @EndDate
BEGIN
   INSERT INTO DIM_DATE (
		DATEKEY,
		[DATE],
		[DAY],
		[WEEKDAY],
		WEEKDAYNAME,
		SHORTWEEKDAYNAME,
		[DAYOFYEAR],
		WEEKOFMONTH,
		WEEKOFYEAR,
		[MONTH],
		[MONTHNAME],
		SHORTMONTHNAME,
		SEASON,
		[QUARTER],
		QUARTERNAME,
		[YEAR],
		MMYYYY,
		MONTHYEAR,
		ISWEEKEND,
		ISHOLIDAY,
		HOLIDAYNAME,
		SPECIALDAYS,
		FINANCIALYEAR,
		FINANCIALQUARTER,
		FINANCIALMONTH,
		FIRSTDATEOFYEAR,
		LASTDATEOFYEAR,
		FIRSTDATEOFQUARTER,
		LASTDATEOFQUARTER,
		FIRSTDATEOFMONTH,
		LASTDATEOFMONTH,
		FIRSTDATEOFWEEK,
		LASTDATEOFWEEK
      )
   SELECT DateKey = YEAR(@CurrentDate) * 10000 + MONTH(@CurrentDate) * 100 + DAY(@CurrentDate),
      [DATE] = @CurrentDate,
      [DAY] = DAY(@CurrentDate),
      [WEEKDAY] = DATEPART(dw, @CurrentDate),
      WEEKDAYNAME = DATENAME(dw, @CurrentDate),
      SHORTWEEKDAYNAME = UPPER(LEFT(DATENAME(dw, @CurrentDate), 3)),
      [DAYOFYEAR] = DATENAME(dy, @CurrentDate),
      [WEEKOFMONTH] = DATEPART(WEEK, @CurrentDate) - DATEPART(WEEK, DATEADD(MM, DATEDIFF(MM, 0, @CurrentDate), 0)) + 1,
      [WEEKOFYEAR] = DATEPART(wk, @CurrentDate),
      [MONTH] = MONTH(@CurrentDate),
      [MONTHNAME] = DATENAME(mm, @CurrentDate),
      [SHORTMONTHNAME] = UPPER(LEFT(DATENAME(mm, @CurrentDate), 3)),
	  SEASON = CASE 
					WHEN MONTH(@CurrentDate) IN (12,1,2)  THEN 'Summer'
					WHEN MONTH(@CurrentDate) IN (3,4,5)   THEN 'Automn'
					WHEN MONTH(@CurrentDate) IN (6,7,8)   THEN 'Winter'
					WHEN MONTH(@CurrentDate) IN (9,10,11) THEN 'Spring'
				END,
      [QUARTER] = DATEPART(q, @CurrentDate),
      [QUARTERNAME] = CASE 
         WHEN DATENAME(qq, @CurrentDate) = 1
            THEN 'First'
         WHEN DATENAME(qq, @CurrentDate) = 2
            THEN 'Second'
         WHEN DATENAME(qq, @CurrentDate) = 3
            THEN 'Third'
         WHEN DATENAME(qq, @CurrentDate) = 4
            THEN 'Fourth'
         END,
      [YEAR] = YEAR(@CurrentDate),
      [MMYYYY] = RIGHT('0' + CAST(MONTH(@CurrentDate) AS VARCHAR(2)), 2) + CAST(YEAR(@CurrentDate) AS VARCHAR(4)),
      [MONTHYEAR] = CAST(YEAR(@CurrentDate) AS VARCHAR(4)) + UPPER(LEFT(DATENAME(mm, @CurrentDate), 3)),
      [ISWEEKEND] = CASE 
         WHEN DATENAME(dw, @CurrentDate) = 'Sunday'
            OR DATENAME(dw, @CurrentDate) = 'Saturday'
            THEN 1
         ELSE 0
         END,
      [ISHOLIDAY] = 0,
	  HOLIDAYNAME = '',
	  SPECIALDAYS = '',
	  FINANCIALYEAR    = NULL,
	  FINANCIALQUARTER = NULL,
	  FINANCIALMONTH   = NULL,
      [FIRSTDATEOFYEAR] = CAST(CAST(YEAR(@CurrentDate) AS VARCHAR(4)) + '-01-01' AS DATE),
      [LASTDATEOFYEAR] = CAST(CAST(YEAR(@CurrentDate) AS VARCHAR(4)) + '-12-31' AS DATE),
      [FIRSTDATEOFQUARTER] = DATEADD(qq, DATEDIFF(qq, 0, GETDATE()), 0),
      [LASTDATEOFQUARTER] = DATEADD(dd, - 1, DATEADD(qq, DATEDIFF(qq, 0, GETDATE()) + 1, 0)),
      [FIRSTDATEOFMONTH] = CAST(CAST(YEAR(@CurrentDate) AS VARCHAR(4)) + '-' + CAST(MONTH(@CurrentDate) AS VARCHAR(2)) + '-01' AS DATE),
      [LASTDATEOFMONTH] = EOMONTH(@CurrentDate),
      [FIRSTDATEOFWEEK] = DATEADD(dd, - (DATEPART(dw, @CurrentDate) - 1), @CurrentDate),
      [LASTDATEOFWEEK] = DATEADD(dd, 7 - (DATEPART(dw, @CurrentDate)), @CurrentDate)
   ----INCREMENT TO NEXT DATE -- FOR PROCESSING----
   SET @CurrentDate = DATEADD(DD, 1, @CurrentDate)
END

--Update Holiday information
UPDATE Dim_Date
SET [IsHoliday] = 1,
   [HOLIDAYNAME] = 'Christmas'
WHERE [Month] = 12
   AND [DAY] = 25

UPDATE Dim_Date
SET SPECIALDAYS = 'Valentines Day'
WHERE [Month] = 2
   AND [DAY] = 14

--Update current date information
/*
UPDATE Dim_Date
SET CurrentYear   = DATEDIFF(yy, GETDATE(), DATE),
    CurrentQuater = DATEDIFF(q, GETDATE() , DATE),
    CurrentMonth  = DATEDIFF(m, GETDATE() , DATE),
    CurrentWeek   = DATEDIFF(ww, GETDATE(), DATE),
    CurrentDay    = DATEDIFF(dd, GETDATE(), DATE)
*/

PRINT '--- Extract Transform and Load ETL Process for DIM_DATE Completed Successfully ---';
GO
/*----------------DIM_DATE ETL Code END----------------*/



/*----------------FACT_SALES ETL Code----------------*/
use CarSales_DW;  --specify target table database for truncating--
TRUNCATE TABLE FACT_SALESTRANSACTION;
use CarSales_DB;  --specify source table database--

---ETL FUNCTIONALITY WITHOUT LOOP---Using Dumping Table SQL---
   --EXTRACTION--TRANSFORMATION--LOADING--ALL IN ONE--
   --Get Minutes : DATEPART(MINUTE, a.TRANTIME) or DATEPART(mi, a.TRANTIME) or DATEPART(n, a.TRANTIME)
   --Get Hours   : DATEPART(HOUR, a.TRANTIME) or DATEPART(hh, a.TRANTIME)
   WITH CTE AS(
	   SELECT ROW_NUMBER() OVER ( ORDER BY a.TRANID ) AS ROWID,
			  a.*, YEAR(a.TRANDATE) * 10000 + MONTH(a.TRANDATE) * 100 + DAY(a.TRANDATE) AS DateKey, 
			  HOURS   = DATEPART(HOUR, a.TRANTIME),
			  MINUTES = DATEPART(MINUTE, a.TRANTIME),
			  TIMEKEY = CASE WHEN   DATEPART(MINUTE, a.TRANTIME) <=30  THEN  DATEPART(HOUR, a.TRANTIME)  ELSE   DATEPART(HOUR, a.TRANTIME)+1   END,
			  b.TRANSACTIONSTATUS, b.DESCRIPTION 
	   FROM SALESTRANSACTION a,
	        TRANSACTIONSTATUS b
			
		WHERE a.STATUSID = b.STATUSID   )
		 
	--PERFORM OPERATIONS : LOAD [ SELECT DESIRED COLUMNS AND LOAD ] --
	INSERT INTO CarSales_DW.dbo.FACT_SALESTRANSACTION 
	SELECT TRANID, DATEKEY, TRANDATE, TIMEKEY, TRANTIME, CUSTOMERID, VEHICLEID, PRICE, QUANTITY, AMOUNT, BRANCHID, STAFFID, STATUSID, TRANSACTIONSTATUS
	FROM CTE;

   ------DONE-----

PRINT '--- Extract Transform and Load ETL Process for FACT_SALESTRANSACTION Completed Successfully ---';
GO
/*--------------End Processing---------------*/




/***----CHECK FOR SUCCESS----***/
use CarSales_DW;
SELECT * FROM DIM_CUSTOMER;
SELECT * FROM DIM_VEHICLE;
SELECT * FROM DIM_STAFF;
SELECT * FROM DIM_BRANCH;
SELECT * FROM DIM_DATE;
SELECT * FROM DIM_TIME;
SELECT * FROM FACT_SALESTRANSACTION;
/**-------------------------**/




