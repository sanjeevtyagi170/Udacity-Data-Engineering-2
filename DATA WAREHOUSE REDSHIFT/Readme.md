# Project Data Warehouse
## Project Overview
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud.

In this project, we will create an ETL pipeline that will extract raw data from s3 and load into staging tables on Redshift.
Then from these Staging Tables a star schema DW will be created on Redshift.

## Schema Design
<center>
<img style="float: center;height:500px;" src="Schema_Design.jpg"><br><br>
</center>




# Queries ran on query editor to check the tables
# Query 1 -  No of male and female users

<center>
<img style="float: center;height:700px;" src="query1.png"><br><br>
</center>

# Query 2 - Find data whose song duration is greater than 100 in the year 2004 and title startswith "M"

<center>
<img style="float: center;height:700px;" src="query2.png"><br><br>
</center>

# Query 3 - Shows the population of all the tables

<center>
<img style="float: center;height:700px;" src="query3a.png"><br><br>
</center>
<center>
<img style="float: center;height:700px;" src="query3b.png"><br><br>
</center>
