--1. Creating the "lay_offs" table

CREATE TABLE lay_offs (
	company  Text, 
	location  Text, 
	industry  Text, 
	total_laid_off  Int, 
	percentage_laid_off  Numeric, 
	date  Date, 
	stage  Text, 
	country  Text, 
	funds_raised_millions  Real
	);



--2. importing records from the source into the table

COPY lay_offs
FROM 'D:\1.layoffs.csv'
DELIMITER ','
CSV HEADER
NULL 'NULL';


-- 3. Quick overview of the dataset

Select *
from lay_offs;


------------------------------ """" CLEANING DATA """"" ------------------------------

--First: Remove duplicates.
--Second: Standardize the data.
--Third: handling Null and blank values.
--Fourth: Remove redundant data.


-- First:  <<< Remove Duplicates >>>

-- Check for duplicate records.
SELECT *, COUNT(*) AS row_frequency
FROM lay_offs
GROUP BY company, location, industry, total_laid_off,
	     percentage_laid_off, date, stage, country, funds_raised_millions
HAVING COUNT(*) > 1
ORDER BY row_frequency DESC;


-- Create a new table with no duplicates

CREATE TABLE layoffs AS 
SELECT DISTINCT *
FROM lay_offs;

-- Delete tha first table containing duplicates
DROP TABLE lay_offs;


--Ensure that records are unique and have no duplicates.
SELECT *, COUNT(*) AS row_frequency
FROM layoffs
GROUP BY company, location, industry, total_laid_off,
	     percentage_laid_off, date, stage, country, funds_raised_millions
HAVING COUNT(*) > 1
ORDER BY row_frequency DESC;


-- Second:  <<< Standardize the data >>>

-- (trim & update) 'company' field
SELECT company, trim(company)
FROM layoffs;

UPDATE layoffs
SET company = trim(company);


-- Check 'industry' field.
SELECT DISTINCT industry
FROM layoffs
	ORDER BY 1;

-- Unify values in 'industry' field
UPDATE layoffs
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


-- Check 'industry' field.
SELECT DISTINCT country
FROM layoffs
	ORDER BY 1 DESC;

-- (Unify & update) values in 'country' field
SELECT DISTINCT country, RTRIM(country, '.')
FROM layoffs
	WHERE country LIKE 'United States%'
ORDER BY 1;

UPDATE layoffs
SET country = RTRIM(country, '.')
WHERE country LIKE 'United States%';



-- Third:  <<< handling Null and blank values >>>

-- Detect missing data in 'industry' field. 
SELECT *
FROM layoffs
WHERE industry IS NULL 
      OR industry = '';

-- Check more deeply for missing data.
SELECT *
FROM layoffs
	WHERE company = 'Airbnb';

-- Unify missing values in 'industry'
UPDATE layoffs
SET industry = NULL
WHERE industry = '';

-- Fill missing data in 'industry' column with the appropriate values based on the activity of the same company.
SELECT t1.industry, t2.industry
FROM layoffs AS t1
JOIN layoffs AS t2
	ON (t1.company = t2.company)
WHERE t1.industry IS NULL 
      AND t2.industry IS NOT NULL;


UPDATE layoffs AS t1
SET industry = t2.industry
FROM layoffs AS t2
WHERE t1.company = t2.company
	AND t1.industry IS NULL
    AND t2.industry IS NOT NULL;


-- Fourthly: <<< Remove redundant data >>>
SELECT *
FROM layoffs
WHERE  total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs
WHERE  total_laid_off IS NULL 
AND percentage_laid_off IS NULL;


-- Getting rid of 'row_num' after it is no longer useful.
ALTER TABLE layoffs
DROP COLUMN row_num;




------------------------------------------- Exploratory DAta --------------------------
SELECT *
FROM layoffs;

SELECT MAX(total_laid_off) , MAX(percentage_laid_off), MAX(funds_raised_millions)
FROM layoffs;

-- Companies with top numbers laid off
SELECT *
FROM layoffs
WHERE total_laid_off = 12000;


-- Companies with top percentage laid off
SELECT *
FROM layoffs
WHERE percentage_laid_off = 1 
ORDER BY total_laid_off DESC NULLS LAST;


-- industries of Top 5 "percentage laid off" 
SELECT industry, COUNT(*)
FROM layoffs
WHERE percentage_laid_off = 1 
GROUP BY industry 
ORDER BY 2 DESC
	LIMIT 5;

-- Countries of Top 5 "percentage laid off" 
SELECT country, COUNT(*)
FROM layoffs
WHERE percentage_laid_off = 1 
GROUP BY country 
ORDER BY 2 DESC
LIMIT 5;

-- Top (percentage_laid_off & funds_raised_millions)
SELECT *
FROM layoffs
WHERE percentage_laid_off = 1 
ORDER BY funds_raised_millions DESC NULLS LAST;

-- companies with grand total_laid_off
SELECT company, SUM(total_laid_off)
FROM layoffs
GROUP BY company
ORDER BY 2 DESC NULLS LAST;


-- industries with grand total_laid_off
SELECT industry, SUM(total_laid_off)
FROM layoffs
GROUP BY industry
	ORDER BY 2 DESC NULLS LAST;


-- countries with grand total_laid_off
SELECT country, SUM(total_laid_off)
FROM layoffs
GROUP BY country
	ORDER BY 2 DESC NULLS LAST;


SELECT MIN(date_), MAX(date_)
FROM layoffs;

-- Years with grand total_laid_off
SELECT EXTRACT(year from date_) AS years, SUM(total_laid_off)
FROM layoffs
WHERE EXTRACT(year from date_) IS NOT NULL
GROUP BY years
ORDER BY 1  ;

-- Stages with grand total_laid_off
SELECT stage, SUM(total_laid_off)
FROM layoffs
GROUP BY 1
ORDER BY 2 DESC NULLS LAST;

-- average 'percentage_laid_off' for each company
SELECT company, ROUND(AVG(percentage_laid_off), 2)
FROM layoffs
GROUP BY 1
ORDER BY 2 DESC NULLS LAST;

-- Grand total_laid_off for each 'month of year'
SELECT EXTRACT(month from date_) AS months, SUM(total_laid_off)
FROM layoffs
GROUP BY months
	ORDER BY 1 DESC NULLS LAST;

-- total_offs ang Rolling total_offs through months
WITH rolling_Total AS 
(
SELECT DATE_TRUNC('month', date_) AS months, SUM(total_laid_off) AS total_offs
FROM layoffs
WHERE 1 IS NOT NULL	
GROUP BY months
ORDER BY 1  NULLS LAST
)
SELECT months, total_offs, SUM(total_offs) OVER(ORDER BY months) AS rolling_total
FROM rolling_Total;

-- Ranking of the highest Companies with top "total layoffs" through past years.
WITH comp_year AS (
SELECT company, EXTRACT(year from date_) AS years, SUM(total_laid_off) AS total_offs
FROM layoffs
GROUP BY company, years), 
comp_rank AS 
(
SELECT *, DENSE_RANK()	OVER(PARTITION BY years ORDER BY total_offs DESC) AS RANKING
FROM comp_year
WHERE years IS NOT NULL 
	AND total_offs IS NOT NULL
)
SELECT * 
FROM comp_rank
WHERE RANKING <= 5;