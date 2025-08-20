-- Day 1
-- Query 1
SELECT * 
FROM sales_data_sample;

-- Query 2
SELECT CUSTOMERNAME, CITY, COUNTRY
FROM sales_data_sample;

-- Query 3
SELECT ORDERNUMBER, CUSTOMERNAME, COUNTRY
FROM sales_data_sample
WHERE COUNTRY = 'France';

-- Query 4
SELECT CUSTOMERNAME, COUNTRY, SALES
FROM sales_data_sample
WHERE COUNTRY = 'France' AND SALES > 5000;

-- Query 5
SELECT ORDERNUMBER, ORDERDATE, STATUS, SALES
FROM sales_data_sample
ORDER BY ORDERDATE DESC;


-- Day 2
-- Query 6
SELECT COUNTRY, COUNT(DISTINCT CUSTOMERNAME) AS total_customers
FROM sales_data_sample
GROUP BY COUNTRY;

-- Query 7
SELECT PRODUCTLINE, SUM(SALES) AS total_sales
FROM sales_data_sample
GROUP BY PRODUCTLINE;

-- Query 8
SELECT CUSTOMERNAME, AVG(SALES) AS avg_order_value
FROM sales_data_sample
GROUP BY CUSTOMERNAME;

-- Query 9
SELECT COUNTRY, COUNT(DISTINCT CUSTOMERNAME) AS total_customers
FROM sales_data_sample
GROUP BY COUNTRY
HAVING COUNT(DISTINCT CUSTOMERNAME) > 10;

-- Query 10
SELECT YEAR_ID, MAX(SALES) AS highest_sale, MIN(SALES) AS lowest_sale
FROM sales_data_sample
GROUP BY YEAR_ID;


-- Day 3
-- Query 11
SELECT ORDERNUMBER, CUSTOMERNAME, PRODUCTCODE, QUANTITYORDERED, SALES
FROM sales_data_sample;

-- Query 12
SELECT CUSTOMERNAME, ORDERNUMBER, SALES
FROM sales_data_sample
ORDER BY CUSTOMERNAME;

-- Query 13
SELECT PRODUCTCODE, PRODUCTLINE, SUM(SALES) AS total_sales
FROM sales_data_sample
GROUP BY PRODUCTCODE, PRODUCTLINE;


-- Day 4
-- Query 14
SELECT 
    s.ORDERNUMBER,
    s.CUSTOMERNAME,
    s.CITY,
    s.COUNTRY,
    s.ORDERDATE,
    s.PRODUCTCODE,
    s.PRODUCTLINE,
    s.QUANTITYORDERED,
    s.PRICEEACH,
    s.SALES
FROM sales_data_sample AS s
ORDER BY s.ORDERDATE;

-- Query 15
SELECT 
    YEAR_ID,
    MONTH_ID,
    DEALSIZE,
    PRODUCTLINE,
    SUM(SALES) AS TotalSales,
    COUNT(DISTINCT CUSTOMERNAME) AS UniqueCustomers
FROM sales_data_sample
GROUP BY YEAR_ID, MONTH_ID, DEALSIZE, PRODUCTLINE
HAVING SUM(SALES) > 10000
ORDER BY YEAR_ID, MONTH_ID, TotalSales DESC;


-- Day 5
-- Query 16
SELECT TOP 5 CUSTOMERNAME, TotalSales
FROM (
    SELECT CUSTOMERNAME, SUM(SALES) AS TotalSales
    FROM sales_data_sample
    GROUP BY CUSTOMERNAME
) AS CustomerSales
ORDER BY TotalSales DESC;

-- Query 17
SELECT ORDERNUMBER, CUSTOMERNAME, SALES
FROM sales_data_sample
WHERE SALES > (SELECT AVG(SALES) FROM sales_data_sample)
ORDER BY SALES DESC;


-- Day 6
-- Query 18
WITH MonthlySales AS (
    SELECT YEAR_ID, MONTH_ID, SUM(SALES) AS TotalSales
    FROM sales_data_sample
    GROUP BY YEAR_ID, MONTH_ID
)
SELECT YEAR_ID, MONTH_ID, TotalSales
FROM MonthlySales
ORDER BY YEAR_ID, MONTH_ID;

-- Query 19
WITH ProductLineSales AS (
    SELECT YEAR_ID, PRODUCTLINE, SUM(SALES) AS TotalSales
    FROM sales_data_sample
    GROUP BY YEAR_ID, PRODUCTLINE
),
RankedLines AS (
    SELECT YEAR_ID, PRODUCTLINE, TotalSales,
           RANK() OVER (PARTITION BY YEAR_ID ORDER BY TotalSales DESC) AS rnk
    FROM ProductLineSales
)
SELECT YEAR_ID, PRODUCTLINE, TotalSales
FROM RankedLines
WHERE rnk = 1;


-- Day 7
-- Query 20
WITH CustomerTotals AS (
    SELECT CUSTOMERNAME, SUM(SALES) AS TotalSales
    FROM sales_data_sample
    GROUP BY CUSTOMERNAME
),
MonthlyTotals AS (
    SELECT YEAR_ID, MONTH_ID, SUM(SALES) AS MonthlySales
    FROM sales_data_sample
    GROUP BY YEAR_ID, MONTH_ID
),
TopProducts AS (
    SELECT YEAR_ID, PRODUCTLINE, SUM(SALES) AS TotalSales,
           RANK() OVER (PARTITION BY YEAR_ID ORDER BY SUM(SALES) DESC) AS rnk
    FROM sales_data_sample
    GROUP BY YEAR_ID, PRODUCTLINE
)
SELECT c.CUSTOMERNAME, c.TotalSales, m.YEAR_ID, m.MONTH_ID, m.MonthlySales,
       tp.PRODUCTLINE AS TopProductLine
FROM CustomerTotals c
JOIN MonthlyTotals m ON m.YEAR_ID IN (2003, 2004)  -- adjust for your dataset years
JOIN TopProducts tp ON tp.YEAR_ID = m.YEAR_ID AND tp.rnk = 1
ORDER BY m.YEAR_ID, m.MONTH_ID, c.TotalSales DESC;
