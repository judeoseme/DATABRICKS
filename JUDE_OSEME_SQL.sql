-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.ls('/FileStore/tables')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Copy file from DBFS to /tmp directory
-- MAGIC dbutils.fs.cp("/FileStore/tables/clinicaltrial_2019.zip", "file:/tmp/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_2021 = spark.read.options(delimiter ="|", header=True, inferSchema=True).csv("/FileStore/tables/clinicaltrial_2021.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_2021.show(5, truncate = False)

-- COMMAND ----------

set clinicaltrialdata = 2021;

-- COMMAND ----------

Creating the Master table for answering Questions.

-- COMMAND ----------

-- Creating a external table named clinicaltrial_2021
CREATE EXTERNAL TABLE if not exists clinicaltrial_2021(id STRING, 
Sponsor string,
Status string, 
Start_date string,
Completion string,
Type string,
Submission string,
Conditions string,
Interventions string)  USING CSV
LOCATION "dbfs:/FileStore/tables/clinicaltrial_${hiveconf:data}.csv"
OPTIONS(delimiter "|",header "true");

-- COMMAND ----------

-- Checking 
select * from clinicaltrial_2021;

-- COMMAND ----------

-- DBTITLE 1,QUESTION 1
--- The number of distinct studies
SELECT COUNT(DISTINCT id) AS Number_of_Studies
FROM clinicaltrial_2021;


-- COMMAND ----------

-- DBTITLE 1,QUESTION 2
-- Select the distinct types and count the number of occurrences of each type
-- Order the results in descending order by the count of occurrences
SELECT Type, COUNT(*) AS Type_Count
FROM clinicaltrial_2021
GROUP BY Type
ORDER BY Type_Count DESC;


-- COMMAND ----------

-- DBTITLE 1,QUESTION 3
-- Create a view to split the conditions using a comma delimiter
CREATE VIEW IF NOT EXISTS condition_table AS
SELECT explode(split(Conditions, ",")) AS conditions
FROM clinicaltrial_2021;

-- Select the conditions from the view, count the number of occurrences of each condition,
-- group the results by condition, and order the results in descending order by the count of occurrences
-- Limit the results to the top 5 conditions
SELECT conditions, COUNT(*) AS Condition_Count
FROM condition_table
GROUP BY conditions
ORDER BY Condition_Count DESC
LIMIT 5;


-- COMMAND ----------

-- DBTITLE 1,QUESTION 4
-- Create an external table for the pharma data
CREATE EXTERNAL TABLE IF NOT EXISTS pharma (
  Company string,
  Parent_Company string,
  Penalty_Amount string,
  Subtraction_From_Penalty string,
  Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting string,
  Penalty_Year string,
  Penalty_Date string,
  Offense_Group string,
  Primary_Offense string,
  Secondary_Offense string,
  Description string,
  Level_of_Government string,
  Action_Type string,
  Agency string,
  Civil_or_Criminal string,
  Prosecution_Agreement string,
  Court string,
  Case_ID string,
  Private_Litigation_Case_Title string,
  Lawsuit_Resolution string,
  Facility_State string,
  City string,
  Address string,
  Zip string,
  NAICS_Code string,
  NAICS_Translation string,
  HQ_Country_of_Parent string,
  HQ_State_of_Parent string,
  Ownership_Structure string,
  Parent_Company_Stock_Ticker string,
  Major_Industry_of_Parent string,
  Specific_Industry_of_Parent string,
  Info_Source string,
  Notes string
)
USING CSV
OPTIONS (delimiter ',', header 'true')
LOCATION 'dbfs:/FileStore/tables/pharma.csv';

-- Create a view by left joining the clinical trial table with the pharma table on a condition
CREATE OR REPLACE VIEW left_join_view AS
SELECT *
FROM clinicaltrial_2021
LEFT JOIN pharma
ON clinicaltrial_2021.Sponsor = pharma.Parent_Company;

-- Select the top 10 sponsors from the left join view by grouping them by sponsor and
-- counting the occurrences of each sponsor, and then ordering the results in descending order
-- based on the count of each sponsor
SELECT Sponsor, COUNT(*) AS sponsor_count
FROM left_join_view
WHERE Parent_Company IS NULL
GROUP BY Sponsor
ORDER BY sponsor_count DESC
LIMIT 10;


-- COMMAND ----------

-- DBTITLE 1,QUESTION 5 NUMBER OF COMPLETED STUDIES EACH MONTH IN A GIVEN YEAR
-- Create a view named Completed_Month by selecting the Completion column and the count of Id column from the clinicaltrial_2021 table.
-- Set the condition where Completion ends with '2021' and Status is 'Completed'.
-- Group the result by Completion column.
CREATE VIEW IF NOT EXISTS Completed_Month AS
SELECT Completion, COUNT(Id) AS StudiesCount
FROM clinicaltrial_2021
WHERE Completion LIKE '%2021' AND Status = 'Completed'
GROUP BY Completion;

-- COMMAND ----------

-- Create a view named LEFT_JOIN_COMPLETION by selecting the first three characters of the Completion column as month
-- and the characters from the 5th to 8th position as year. Also include the Completion column and StudiesCount column from the Completed_Month view.
CREATE VIEW IF NOT EXISTS LEFT_JOIN_COMPLETION AS
SELECT LEFT(Completion, 3) AS month, SUBSTRING(Completion, 5, 8) AS year, Completion, StudiesCount
FROM Completed_Month;

-- COMMAND ----------

-- Creating a view to convert the previous view columns datatype to timestamp and order it.
CREATE VIEW IF NOT EXISTS Time_Stamp AS
SELECT from_unixtime(unix_timestamp(concat('01-', month, '-', year), 'dd-MMM-yyyy'), 'dd-MM-yyyy') AS Date_col,
month,
StudiesCount
FROM LEFT_JOIN_COMPLETION
ORDER BY substring(Date_col, 4, 5);

-- COMMAND ----------

-- Selecting only the month and the count from the previos view 
Select month, StudiesCount from Time_Stamp

-- COMMAND ----------

-- Plottoig a bar chart
Select month, StudiesCount from Time_Stamp

-- COMMAND ----------

-- DBTITLE 1,FURTHER ANALYSIS
Finding the top 10 Sponsors count who are suspended
SELECT Sponsor,count(Status) AS Status_Count
FROM clinicaltrial_2021 where Status  == "Suspended"
GROUP BY Sponsor 
ORDER BY Status_Count DESC LIMIT 10;

-- COMMAND ----------


