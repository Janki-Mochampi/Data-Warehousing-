/*Question 1: What are total seasonal sales of AZ Motors by Vehicle make,branch and country?,*/

SELECT b.COUNTRY, b.BRANCHNAME, d.SEASON, c.MAKE, SUM(a.AMOUNT) AS TOTALSALES
FROM FACT_SALESTRANSACTION   a,
		 DIM_BRANCH   b,
		 DIM_VEHICLE  c,
		 DIM_DATE     d
		 
WHERE a.BRANCHID     = b.BRANCHID AND
		  a.VEHICLEID = c.VEHICLEID AND 
		  a.DATEKEY     = d.DATEKEY 
		    
GROUP BY
		ROLLUP(b.COUNTRY, b.BRANCHNAME, d.SEASON, c.MAKE)


/*Question 2: Who are our loyal customers, what are their demographic properties; (age groups, gender, country),
what model of vehicles do they mainly buy?*/
SELECT b.TITLE+' '+ b.FULLNAME AS CUSTOMERNAME, b.AGEGROUP,b.COUNTRY,b.GENDER, c.MODEL, SUM(a.AMOUNT) AS TOTALSALES
FROM FACT_SALESTRANSACTION a,
	 DIM_CUSTOMER b,
	 DIM_VEHICLE c
WHERE a.CUSTOMERID  = b.CUSTOMERID    AND
	  a.VEHICLEID = c.VEHICLEID AND
	  a.STATUSID = 1  --NON-CANCELLED TRANSACTIONS
GROUP BY
	   ROLLUP(b.TITLE+' '+ b.FULLNAME, b.AGEGROUP,b.COUNTRY,b.GENDER, c.MODEL)



/*Question 3: Which Vehicle makes generates the largest volume of revenue, at which Branches, in which Country*/
SELECT c.BRANCHNAME, c.COUNTRY, d.MAKE, SUM(a.AMOUNT) AS TOTALSALES
FROM FACT_SALESTRANSACTION a,
	 DIM_BRANCH c,
	 DIM_VEHICLE d
WHERE a.BRANCHID  = c.BRANCHID  AND
	  a.VEHICLEID = d.VEHICLEID AND
	  a.STATUSID = 1  --NON-CANCELLED TRANSACTIONS
GROUP BY 
	   ROLLUP(c.BRANCHNAME, c.COUNTRY, d.MAKE);


/*Question 4: Which Branch has the largest volume of returned goods, for which Vehicle makes in which Country*/
SELECT b.COUNTRY, b.BRANCHNAME, d.MAKE, SUM(a.AMOUNT) AS RETURNED_PRODUCTS_SALES
FROM FACT_SALESTRANSACTION a,
     DIM_BRANCH b,
	 DIM_VEHICLE d
WHERE a.BRANCHID     = b.BRANCHID     AND
	  a.VEHICLEID = d.VEHICLEID AND
	   a.STATUSID   = 0  --CANCELLED TRANSACTIONS
GROUP BY 
	   ROLLUP(b.COUNTRY, b.BRANCHNAME, d.MAKE);


/*Question 5: Who are our staff, what are their demographic properties (age groups, gender, country) and in which branches do they work*/

SELECT	a.TITLE, a.FIRSTNAME, a.SURNAME, AGEGROUP, a.GENDER, a.STAFFSTATUS, a.POSITIONNAME, a.BRANCHNAME
FROM    DIM_STAFF a










